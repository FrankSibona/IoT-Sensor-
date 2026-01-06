import time
import threading
from datetime import datetime, timedelta

from sqlalchemy import select

from .db import SessionLocal
from .models import Device
from . import crud

class PresenceWatchdog:
    def __init__(self, offline_after_s: int = 900, check_every_s: int = 60):
        self.offline_after_s = offline_after_s
        self.check_every_s = check_every_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            time.sleep(self.check_every_s)

            cutoff = datetime.utcnow() - timedelta(seconds=self.offline_after_s)
            db = SessionLocal()
            try:
                # candidatos: ONLINE con last_seen viejo
                q = (
                    select(Device)
                    .where(Device.status == "ONLINE")
                    .where(Device.last_seen.is_not(None))
                    .where(Device.last_seen < cutoff)
                )
                devices = list(db.scalars(q).all())

                for dev in devices:
                    crud.set_device_offline(db, dev.device_id)
                    crud.create_alarm_simple(
                        db,
                        dev.device_id,
                        code="DEVICE_OFFLINE",
                        severity="WARN",
                        message=f"Equipo sin reportar desde {dev.last_seen} (UTC).",
                    )
            finally:
                db.close()


presence_watchdog = PresenceWatchdog(
    offline_after_s=900,   # 15 min
    check_every_s=60,      # check cada 1 min
)
