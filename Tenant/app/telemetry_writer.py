# app/telemetry_writer.py
import time
import threading
from datetime import datetime, timedelta

from sqlalchemy import select

from .db import SessionLocal
from . import crud
from .models import Alarm
from .diagnostico_engine import DiagnosticoEngine

# Engine singleton (mantiene estado por device_id)
engine = DiagnosticoEngine()


# =========================
# Helpers
# =========================
def _cfg_to_dict(cfg_obj) -> dict:
    if cfg_obj is None:
        return {}
    if isinstance(cfg_obj, dict):
        return cfg_obj
    try:
        return {c.name: getattr(cfg_obj, c.name) for c in cfg_obj.__table__.columns}
    except Exception:
        return {k: getattr(cfg_obj, k) for k in dir(cfg_obj) if not k.startswith("_")}


def _alarm_recent(db, device_id: str, code: str, minutes: int) -> bool:
    """
    Retorna True si ya existe una alarma con el mismo code
    para el device dentro de los últimos `minutes`.
    """
    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    q = (
        select(Alarm)
        .where(
            Alarm.device_id == device_id,
            Alarm.code == code,
            Alarm.ts >= cutoff,
        )
        .limit(1)
    )
    return db.scalars(q).first() is not None


# =========================
# Telemetry Writer
# =========================
class TelemetryWriter:
    """
    Writer con:
      - rate limit por device (min_interval_s)
      - buffer en memoria
      - commit por batch_size o flush_interval_s

    + Diagnóstico:
      - corre al mismo ritmo que se escribe (cuando pasa el rate-limit)
      - usa snapshot (últimos valores) y cfg por equipo
      - solo genera ALARMAS/RECS
      - cooldown por código (anti-spam)
    """

    COOLDOWN_MINUTES = 15  # <<< AJUSTABLE

    def __init__(self, batch_size: int = 200, flush_interval_s: float = 2.0, min_interval_s: float = 5.0):
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.min_interval_s = min_interval_s

        self._lock = threading.Lock()
        self._buf: list[dict] = []
        self._last_write_ts_by_dev: dict[str, float] = {}
        self._last_snapshot_by_dev: dict[str, dict] = {}

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # =========================
    # Lifecycle
    # =========================
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    # =========================
    # Ingest
    # =========================
    def enqueue(self, device_id: str, tele: dict | None = None, state: dict | None = None):
        tele = tele or {}
        state = state or {}

        now_ts = time.time()
        now_dt = datetime.utcnow()

        # --- Snapshot + rate-limit ---
        with self._lock:
            last_ts = self._last_write_ts_by_dev.get(device_id, 0.0)
            if (now_ts - last_ts) < self.min_interval_s:
                snap = self._last_snapshot_by_dev.setdefault(device_id, {})
                snap.update(tele)
                snap.update(state)
                return

            snap = self._last_snapshot_by_dev.setdefault(device_id, {})
            snap.update(tele)
            snap.update(state)

            row = {
                "ts": now_dt,
                "device_id": device_id,

                # tele
                "presostat_low": snap.get("presostat_low"),
                "pressure_inter": snap.get("pressure_inter"),
                "flow_perm": snap.get("flow_perm"),
                "flow_reject": snap.get("flow_reject"),
                "conductivity_perm": snap.get("conductivity_perm"),
                "pressure_feed": snap.get("pressure_feed"),
                "temperature_feed": snap.get("temperature_feed"),

                # state
                "float_call": snap.get("float_call"),
                "valve_feed": snap.get("valve_feed"),
                "pump_low": snap.get("pump_low"),
                "pump_high": snap.get("pump_high"),
            }

            self._buf.append(row)
            self._last_write_ts_by_dev[device_id] = now_ts
            snap_copy = dict(snap)

        # --- Diagnóstico (fuera del lock) ---
        self._run_diagnosis(device_id, snap_copy, now_ts, now_dt)

    # =========================
    # Diagnóstico + Cooldown
    # =========================
    def _run_diagnosis(self, device_id: str, snap: dict, now_ts: float, now_dt: datetime):
        db = None
        try:
            db = SessionLocal()

            cfg_db = _cfg_to_dict(crud.get_config(db, device_id))

            flow_perm_nom = cfg_db.get("flow_perm_nom") or 0.0
            flow_rej_nom = cfg_db.get("flow_rej_nom") or 0.0

            cfg = {
                "ref_pint_work": cfg_db.get("p_inter_nom"),
                "ref_qp": cfg_db.get("flow_perm_nom"),
                "ref_qr": cfg_db.get("flow_rej_nom"),
                "ref_qsum": (flow_perm_nom + flow_rej_nom) or None,
                "ref_ct": cfg_db.get("cond_perm_nom"),

                "t_precheck_s": cfg_db.get("start_wait_presostat_s", 30),
                "t_stable_s": cfg_db.get("stabilize_pressure_s", 180),
                "t_start_ignore_s": (cfg_db.get("cond_ignore_minutes", 1) or 0) * 60,
                "w_avg_s": 60,
                "t_persist_s": 120,

                "pint_pre_zero_th": cfg_db.get("p_transducer_threshold", 0.05),

                "k_pe": "presostat_low",
                "k_pint": "pressure_inter",
                "k_qp": "flow_perm",
                "k_qr": "flow_reject",
                "k_ct": "conductivity_perm",
            }

            live = {
                "presostat_low": {"value": snap.get("presostat_low"), "ts": now_ts},
                "pressure_inter": {"value": snap.get("pressure_inter"), "ts": now_ts},
                "flow_perm": {"value": snap.get("flow_perm"), "ts": now_ts},
                "flow_reject": {"value": snap.get("flow_reject"), "ts": now_ts},
                "conductivity_perm": {"value": snap.get("conductivity_perm"), "ts": now_ts},
            }

            res = engine.evaluate(device_id, live, cfg)

            # ---- ALARMAS (con cooldown) ----
            for a in res.alarms:
                if _alarm_recent(db, device_id, a.code, self.COOLDOWN_MINUTES):
                    continue

                crud.insert_alarm(
                    db,
                    device_id=device_id,
                    code=a.code,
                    severity=a.severity,
                    title=a.title,
                    detail=a.detail,
                    ts=now_dt,
                )

            # ---- RECOMENDACIONES (mismo cooldown) ----
            for r in res.recs:
                rec_code = f"REC_{r.code}"
                if _alarm_recent(db, device_id, rec_code, self.COOLDOWN_MINUTES):
                    continue

                crud.insert_recommendation(
                    db,
                    device_id=device_id,
                    code=r.code,
                    title=r.title,
                    detail=r.detail,
                    ts=now_dt,
                )

        except Exception as e:
            print(f"[DIAG] error en {device_id}: {e}")

        finally:
            if db is not None:
                db.close()

    # =========================
    # Flush thread
    # =========================
    def flush(self):
        with self._lock:
            if not self._buf:
                return
            batch = self._buf[:]
            self._buf.clear()

        db = SessionLocal()
        try:
            crud.insert_telemetry_batch(db, batch)
        finally:
            db.close()

    def _run(self):
        last_flush = time.time()
        while not self._stop.is_set():
            time.sleep(0.2)
            now = time.time()

            do_flush = False
            with self._lock:
                if len(self._buf) >= self.batch_size:
                    do_flush = True
                elif (now - last_flush) >= self.flush_interval_s and len(self._buf) > 0:
                    do_flush = True

            if do_flush:
                self.flush()
                last_flush = now


telemetry_writer = TelemetryWriter(
    batch_size=200,
    flush_interval_s=2.0,
    min_interval_s=5.0,
)
