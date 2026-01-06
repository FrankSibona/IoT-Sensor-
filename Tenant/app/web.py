# app/web.py  (COMPLETO - EL QUE YA TENÉS, CON publish() USANDO mqtt_client)
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from jinja2 import ChoiceLoader, FileSystemLoader
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates

from . import crud
from .db import get_db
from .mqtt_client import get_publisher

# =========================
# Templates
# =========================
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.loader = ChoiceLoader([FileSystemLoader(str(TEMPLATES_DIR))])

router = APIRouter()


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_db_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _require(min_v: float, nom_v: float, max_v: float, field: str) -> None:
    if min_v > nom_v or nom_v > max_v:
        raise HTTPException(
            status_code=400,
            detail=f"Rango inválido en '{field}': debe cumplirse min <= nom <= max",
        )


# =========================
# HOME
# =========================
@router.get("/")
def home() -> RedirectResponse:
    return RedirectResponse(url="/devices", status_code=303)


# =========================
# TENANTS
# =========================
@router.get("/tenants")
def tenants_list(request: Request, db: Session = Depends(get_db)):
    tenants = crud.list_tenants(db)
    return templates.TemplateResponse(
        "tenants.html",
        {"request": request, "tenants": tenants},
    )


@router.post("/tenants")
def tenants_create(name: str = Form(...), db: Session = Depends(get_db)) -> RedirectResponse:
    name = (name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Nombre inválido")

    crud.create_tenant(db, name)
    return RedirectResponse(url="/tenants", status_code=303)


# =========================
# DEVICES
# =========================
@router.get("/devices")
def devices_list(request: Request, db: Session = Depends(get_db)):
    devices = crud.list_devices(db)

    tenants = crud.list_tenants(db)
    tenants_by_id = {t.id: t for t in tenants}

    now = _utcnow_naive()
    seconds_since_seen: Dict[str, Optional[int]] = {}

    for d in devices:
        last_seen = _normalize_db_dt(getattr(d, "last_seen", None))
        if last_seen is None:
            seconds_since_seen[d.device_id] = None
        else:
            seconds_since_seen[d.device_id] = int((now - last_seen).total_seconds())

    # si no la tenés, podés borrarlo del template y de acá:
    try:
        last_ack_by_device = crud.last_cmd_ack_by_device(db, [d.device_id for d in devices])
    except Exception:
        last_ack_by_device = {}

    return templates.TemplateResponse(
        "devices.html",
        {
            "request": request,
            "devices": devices,
            "tenants_by_id": tenants_by_id,
            "seconds_since_seen": seconds_since_seen,
            "now_utc": now,
            "last_ack_by_device": last_ack_by_device,
        },
    )


@router.get("/devices/new")
def devices_new_form(request: Request, db: Session = Depends(get_db)):
    tenants = crud.list_tenants(db)
    return templates.TemplateResponse(
        "device_new.html",
        {"request": request, "tenants": tenants},
    )


@router.post("/devices/new")
def devices_create(device_id: str = Form(...), tenant_id: int = Form(...), db: Session = Depends(get_db)) -> RedirectResponse:
    device_id = (device_id or "").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="device_id inválido")
    if tenant_id <= 0:
        raise HTTPException(status_code=400, detail="tenant_id inválido")

    tenant = crud.get_tenant(db, tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant no encontrado")

    crud.create_device(db, device_id, tenant_id)
    return RedirectResponse(url="/devices", status_code=303)


# =========================
# DEVICE CONFIG
# =========================
@router.get("/devices/{device_id}/config")
def device_config_form(device_id: str, request: Request, db: Session = Depends(get_db)):
    dev = crud.get_device(db, device_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Device no encontrado")

    cfg = crud.get_config(db, device_id)
    return templates.TemplateResponse(
        "device_config.html",
        {"request": request, "dev": dev, "cfg": cfg},
    )


@router.post("/devices/{device_id}/config")
def device_config_save(
    device_id: str,
    db: Session = Depends(get_db),
    flow_perm_nom: float = Form(...),
    flow_perm_min: float = Form(...),
    flow_perm_max: float = Form(...),
    flow_rej_nom: float = Form(...),
    flow_rej_min: float = Form(...),
    flow_rej_max: float = Form(...),
    p_inter_nom: float = Form(...),
    p_inter_min: float = Form(...),
    p_inter_max: float = Form(...),
    cond_perm_nom: float = Form(...),
    cond_perm_max: float = Form(...),
    cond_high_pct: float = Form(...),
    cond_ignore_minutes: int = Form(...),
    stabilize_pressure_s: int = Form(...),
    start_wait_presostat_s: int = Form(...),
    high_pressure_instant_pct: float = Form(...),
    high_pressure_instant_s: int = Form(...),
    p_transducer_threshold: float = Form(...),
) -> RedirectResponse:
    dev = crud.get_device(db, device_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Device no encontrado")

    _require(flow_perm_min, flow_perm_nom, flow_perm_max, "flow_perm")
    _require(flow_rej_min, flow_rej_nom, flow_rej_max, "flow_rej")
    _require(p_inter_min, p_inter_nom, p_inter_max, "p_inter")

    if cond_perm_max < cond_perm_nom:
        raise HTTPException(status_code=400, detail="Rango inválido en 'cond_perm': debe cumplirse nom <= max")
    if cond_ignore_minutes < 0:
        raise HTTPException(status_code=400, detail="cond_ignore_minutes inválido")
    for name, v in [
        ("stabilize_pressure_s", stabilize_pressure_s),
        ("start_wait_presostat_s", start_wait_presostat_s),
        ("high_pressure_instant_s", high_pressure_instant_s),
    ]:
        if v < 0:
            raise HTTPException(status_code=400, detail=f"{name} inválido")

    data: Dict[str, Any] = {
        "flow_perm_nom": flow_perm_nom,
        "flow_perm_min": flow_perm_min,
        "flow_perm_max": flow_perm_max,
        "flow_rej_nom": flow_rej_nom,
        "flow_rej_min": flow_rej_min,
        "flow_rej_max": flow_rej_max,
        "p_inter_nom": p_inter_nom,
        "p_inter_min": p_inter_min,
        "p_inter_max": p_inter_max,
        "cond_perm_nom": cond_perm_nom,
        "cond_perm_max": cond_perm_max,
        "cond_high_pct": cond_high_pct,
        "cond_ignore_minutes": cond_ignore_minutes,
        "stabilize_pressure_s": stabilize_pressure_s,
        "start_wait_presostat_s": start_wait_presostat_s,
        "high_pressure_instant_pct": high_pressure_instant_pct,
        "high_pressure_instant_s": high_pressure_instant_s,
        "p_transducer_threshold": p_transducer_threshold,
    }

    crud.update_config(db, device_id, data)
    return RedirectResponse(url=f"/devices/{device_id}/config", status_code=303)


# =========================
# DEVICE ALARMS
# =========================
@router.get("/devices/{device_id}/alarms")
def device_alarms(device_id: str, request: Request, db: Session = Depends(get_db)):
    dev = crud.get_device(db, device_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Device no encontrado")

    alarms = crud.list_alarms_for_device(db, device_id, limit=300)
    return templates.TemplateResponse(
        "device_alarms.html",
        {"request": request, "dev": dev, "alarms": alarms},
    )


# =========================
# DEVICE TELEMETRY
# =========================
@router.get("/devices/{device_id}/telemetry")
def device_telemetry(device_id: str, request: Request, db: Session = Depends(get_db)):
    dev = crud.get_device(db, device_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Device no encontrado")

    rows = crud.list_telemetry_for_device(db, device_id, limit=500)
    return templates.TemplateResponse(
        "device_telemetry.html",
        {"request": request, "dev": dev, "rows": rows},
    )


# =========================
# DEVICE COMMANDS (MQTT publish)
# =========================
@router.post("/devices/{device_id}/command")
def device_command_publish(
    device_id: str,
    db: Session = Depends(get_db),
    topic: str = Form(...),
    payload: str = Form(...),
    qos: int = Form(0),
    retain: str = Form("false"),
) -> RedirectResponse:
    dev = crud.get_device(db, device_id)
    if not dev:
        raise HTTPException(status_code=404, detail="Device no encontrado")

    topic = (topic or "").strip()
    if not topic:
        raise HTTPException(status_code=400, detail="Topic inválido")

    if qos not in (0, 1, 2):
        raise HTTPException(status_code=400, detail="QoS inválido (0/1/2)")

    retain_bool = str(retain).lower() in ("1", "true", "yes", "on")

    publisher = get_publisher()
    try:
        publisher.publish(topic, payload, qos=qos, retain=retain_bool)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error publicando MQTT: {e}")

    return RedirectResponse(url="/devices", status_code=303)




# =========================
# DEBUG TELEMETRY INGEST
# =========================
from fastapi import Body
from .telemetry_writer import telemetry_writer

@router.post("/debug/devices/{device_id}/push")
def debug_push(device_id: str, payload: Dict[str, Any] = Body(...)):
    """
    Endpoint SOLO para pruebas.
    Inyecta telemetría manualmente al TelemetryWriter.
    """
    telemetry_writer.enqueue(device_id, tele=payload, state={})
    return {"ok": True, "queued": True}

