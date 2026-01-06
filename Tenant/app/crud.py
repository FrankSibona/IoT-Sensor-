# app/crud.py  (REEMPLAZAR COMPLETO)
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Alarm, Device, DeviceConfig, Telemetry, Tenant
from .settings import settings


# =========================
# Helpers
# =========================
def slugify(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "tenant"


# =========================
# Tenants
# =========================
def list_tenants(db: Session) -> list[Tenant]:
    return list(db.scalars(select(Tenant).order_by(Tenant.name)).all())


def create_tenant(db: Session, name: str) -> Tenant:
    t = Tenant(name=name.strip())
    db.add(t)
    db.commit()
    db.refresh(t)
    return t


def get_tenant(db: Session, tenant_id: int) -> Optional[Tenant]:
    return db.get(Tenant, tenant_id)


# =========================
# Devices
# =========================
def list_devices(db: Session) -> list[Device]:
    return list(db.scalars(select(Device).order_by(Device.created_at.desc())).all())


def get_device(db: Session, device_id: str) -> Optional[Device]:
    return db.get(Device, device_id)


def create_device(db: Session, device_id: str, tenant_id: int) -> Device:
    device_id = (device_id or "").strip()
    if not device_id:
        raise ValueError("device_id inválido")

    tenant = get_tenant(db, tenant_id)
    if not tenant:
        raise ValueError("Tenant inválido")

    tenant_slug = slugify(tenant.name)
    topic_prefix = f"{settings.TOPIC_ROOT}/{tenant_slug}/{device_id}"

    d = Device(
        device_id=device_id,
        tenant_id=tenant_id,
        status="PENDING",
        topic_prefix=topic_prefix,
        created_at=datetime.utcnow(),
    )
    db.add(d)

    cfg = DeviceConfig(device_id=device_id)
    db.add(cfg)

    db.commit()
    db.refresh(d)
    return d


def touch_device_seen(db: Session, device_id: str) -> Tuple[bool, Optional[str]]:
    """
    Actualiza last_seen y setea status ONLINE.
    Retorna: (first_seen, previous_status)
    """
    dev = get_device(db, device_id)
    if not dev:
        return (False, None)

    first_seen = dev.last_seen is None
    prev_status = dev.status

    dev.last_seen = datetime.utcnow()
    dev.status = "ONLINE"
    db.commit()
    return (first_seen, prev_status)


def set_device_offline(db: Session, device_id: str) -> bool:
    dev = get_device(db, device_id)
    if not dev:
        return False
    dev.status = "OFFLINE"
    db.commit()
    return True


def list_devices_stale(db: Session, cutoff: datetime) -> list[Device]:
    q = select(Device).where(Device.last_seen.is_not(None), Device.last_seen < cutoff)
    return list(db.scalars(q).all())


# =========================
# Config
# =========================
def get_config(db: Session, device_id: str) -> Optional[DeviceConfig]:
    return db.get(DeviceConfig, device_id)


def update_config(db: Session, device_id: str, data: dict) -> DeviceConfig:
    cfg = get_config(db, device_id)
    if not cfg:
        raise ValueError("Config no encontrada")

    for k, v in (data or {}).items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)

    db.commit()
    db.refresh(cfg)
    return cfg


# =========================
# Alarms
# =========================
def create_alarm(
    db: Session,
    device_id: str,
    code: str,
    severity: str,
    message: str,
    data: Optional[dict] = None,
) -> Alarm:
    a = Alarm(
        device_id=device_id,
        code=code,
        severity=severity,
        message=message,
        data_json=json.dumps(data or {}, ensure_ascii=False),
    )
    db.add(a)
    db.commit()
    db.refresh(a)
    return a


def create_alarm_simple(db: Session, device_id: str, code: str, severity: str, message: str) -> Alarm:
    a = Alarm(device_id=device_id, code=code, severity=severity, message=message)
    db.add(a)
    db.commit()
    db.refresh(a)
    return a


def list_alarms_for_device(db: Session, device_id: str, limit: int = 200) -> list[Alarm]:
    q = (
        select(Alarm)
        .where(Alarm.device_id == device_id)
        .order_by(Alarm.ts.desc())
        .limit(limit)
    )
    return list(db.scalars(q).all())


def last_cmd_ack_by_device(db: Session, device_ids: list[str]) -> dict[str, Alarm]:
    out: dict[str, Alarm] = {}
    for did in device_ids:
        q = (
            select(Alarm)
            .where(Alarm.device_id == did, Alarm.code == "CMD_ACK")
            .order_by(Alarm.ts.desc())
            .limit(1)
        )
        a = db.scalars(q).first()
        if a:
            out[did] = a
    return out


# =========================
# Compat: API usada por TelemetryWriter/Diagnóstico
# =========================
def insert_alarm(
    db: Session,
    device_id: str,
    code: str,
    severity: str,
    title: str,
    detail: str = "",
    ts: Optional[datetime] = None,
) -> Alarm:
    """
    Adaptador para el engine nuevo.
    Mapea (title+detail) -> message y guarda ts si tu modelo lo soporta.
    """
    msg = title if not detail else f"{title}\n{detail}"
    a = Alarm(
        device_id=device_id,
        code=code,
        severity=severity,
        message=msg,
        data_json=None,
    )
    # Si tu modelo Alarm tiene columna ts, la seteamos.
    if ts is not None and hasattr(a, "ts"):
        setattr(a, "ts", ts)

    db.add(a)
    db.commit()
    db.refresh(a)
    return a


def insert_recommendation(
    db: Session,
    device_id: str,
    code: str,
    title: str,
    detail: str = "",
    ts: Optional[datetime] = None,
) -> Alarm:
    """
    No existe tabla Recommendation en tu esquema actual.
    Para no romper, guardamos recomendaciones como Alarm con code prefijado.
    """
    msg = title if not detail else f"{title}\n{detail}"
    a = Alarm(
        device_id=device_id,
        code=f"REC_{code}",
        severity="info",
        message=msg,
        data_json=None,
    )
    if ts is not None and hasattr(a, "ts"):
        setattr(a, "ts", ts)

    db.add(a)
    db.commit()
    db.refresh(a)
    return a


# =========================
# Telemetry
# =========================
def insert_telemetry_batch(db: Session, rows: list[dict]) -> int:
    objs = [Telemetry(**r) for r in rows]
    db.add_all(objs)
    db.commit()
    return len(objs)


def list_telemetry_for_device(db: Session, device_id: str, limit: int = 500) -> list[Telemetry]:
    q = (
        select(Telemetry)
        .where(Telemetry.device_id == device_id)
        .order_by(Telemetry.ts.desc())
        .limit(limit)
    )
    return list(db.scalars(q).all())


def cleanup_telemetry(db: Session, retention_days: int) -> int:
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    q = db.query(Telemetry).filter(Telemetry.ts < cutoff)
    n = q.count()
    q.delete(synchronize_session=False)
    db.commit()
    return n
