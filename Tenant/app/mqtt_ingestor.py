# app/ingestor.py  (REEMPLAZAR COMPLETO)
import json
import threading
import ssl
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt

from .settings import settings
from .engine import engine
from .telemetry_writer import telemetry_writer
from .db import SessionLocal
from . import crud

# =========================
# Constantes
# =========================
TELE_KEYS = {
    "presostat_low",
    "pressure_inter",
    "flow_perm",
    "flow_reject",
    "conductivity_perm",
    "pressure_feed",
    "temperature_feed",
}

STATE_KEYS = {"float_call", "valve_feed", "pump_low", "pump_high", "mode"}

PRESENCE_TOUCH_EVERY_S = 30     # no tocar DB más de 1 vez cada 30s por equipo
OFFLINE_AFTER_S = 180           # 3 min sin reportar => OFFLINE
OFFLINE_SCAN_EVERY_S = 30       # scan cada 30s

_last_touch_by_device = {}
_last_touch_lock = threading.Lock()


# =========================
# Utils
# =========================
def _should_touch(device_id: str) -> bool:
    now = time.time()
    with _last_touch_lock:
        last = _last_touch_by_device.get(device_id)
        if last is None or (now - last) >= PRESENCE_TOUCH_EVERY_S:
            _last_touch_by_device[device_id] = now
            return True
    return False


def _extract_parts(topic: str):
    # <root>/<tenant>/<device_id>/<kind>/...
    parts = topic.split("/")
    if len(parts) < 4:
        return None
    if parts[0] != settings.TOPIC_ROOT:
        return None
    return parts[1], parts[2], parts[3], parts[4:]


def _decode_payload(raw: bytes):
    s = raw.decode("utf-8", errors="ignore").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s


def _tele_value(payload):
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, dict) and "v" in payload and isinstance(payload["v"], (int, float)):
        return float(payload["v"])
    return None


# =========================
# Presencia
# =========================
def _presence_touch_and_events(device_id: str):
    db = SessionLocal()
    try:
        first_seen, prev_status = crud.touch_device_seen(db, device_id)

        if first_seen:
            crud.create_alarm_simple(
                db,
                device_id,
                code="DEVICE_FIRST_SEEN",
                severity="INFO",
                message="Equipo comenzó a reportar por primera vez.",
            )
        elif prev_status == "OFFLINE":
            crud.create_alarm_simple(
                db,
                device_id,
                code="DEVICE_RECONNECTED",
                severity="INFO",
                message="Equipo volvió a reportar (reconectado).",
            )
    finally:
        db.close()


# =========================
# MQTT callbacks
# =========================
def _on_connect(client, userdata, flags, rc, properties=None):
    root = settings.TOPIC_ROOT
    client.subscribe(f"{root}/+/+/tele/#")
    client.subscribe(f"{root}/+/+/state")
    client.subscribe(f"{root}/+/+/event")


def _on_message(client, userdata, msg):
    parts = _extract_parts(msg.topic)
    if not parts:
        return
    tenant, device_id, kind, rest = parts

    # presencia (throttled)
    if _should_touch(device_id):
        _presence_touch_and_events(device_id)

    payload = _decode_payload(msg.payload)

    if kind == "tele":
        if not rest:
            return
        key = rest[0]
        if key not in TELE_KEYS:
            return
        v = _tele_value(payload)
        if v is None:
            return

        engine.ingest(device_id, tele={key: v})
        telemetry_writer.enqueue(device_id, tele={key: v})
        return

    if kind == "state":
        if not isinstance(payload, dict):
            return
        state = {k: payload[k] for k in STATE_KEYS if k in payload}

        engine.ingest(device_id, state=state)
        telemetry_writer.enqueue(device_id, state=state)
        return

    if kind == "event":
        if isinstance(payload, dict) and "name" in payload:
            name = str(payload["name"])
            engine.ingest(device_id, event={"name": name})

            # Guardar ACK como alarma para mostrar en /devices
            if name.upper() == "ACK":
                db = SessionLocal()
                try:
                    cmd = str(payload.get("cmd", ""))
                    ok = payload.get("ok", None)
                    msg_txt = f"ACK cmd={cmd} ok={ok}"
                    crud.create_alarm(
                        db,
                        device_id=device_id,
                        code="CMD_ACK",
                        severity="INFO",
                        message=msg_txt,
                        data=payload,
                    )
                finally:
                    db.close()
        return


# =========================
# OFFLINE watchdog
# =========================
def _offline_watchdog():
    while True:
        try:
            cutoff = datetime.utcnow() - timedelta(seconds=OFFLINE_AFTER_S)
            db = SessionLocal()
            try:
                stale = crud.list_devices_stale(db, cutoff)
                for dev in stale:
                    if dev.status != "OFFLINE":
                        crud.set_device_offline(db, dev.device_id)
                        crud.create_alarm_simple(
                            db,
                            dev.device_id,
                            code="DEVICE_OFFLINE",
                            severity="WARN",
                            message=f"Equipo sin reportar por más de {OFFLINE_AFTER_S}s.",
                        )
            finally:
                db.close()
        except Exception:
            pass

        time.sleep(OFFLINE_SCAN_EVERY_S)


# =========================
# Start
# =========================
def start_mqtt():
    telemetry_writer.start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if settings.MQTT_USER and settings.MQTT_PASS:
        client.username_pw_set(settings.MQTT_USER, settings.MQTT_PASS)

    if settings.MQTT_TLS:
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

    client.on_connect = _on_connect
    client.on_message = _on_message

    # watchdog OFFLINE
    threading.Thread(target=_offline_watchdog, daemon=True).start()

    def _run():
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT, keepalive=60)
        client.loop_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()
    return client, th
