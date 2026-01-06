from datetime import datetime
from sqlalchemy import String, Integer, Float, DateTime, ForeignKey, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base
from sqlalchemy import Float

class Tenant(Base):
    __tablename__ = "tenants"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)

    devices: Mapped[list["Device"]] = relationship(back_populates="tenant")

class Device(Base):
    __tablename__ = "devices"
    device_id: Mapped[str] = mapped_column(String(40), primary_key=True)  # e.g. RO-2025-0001
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id"), nullable=False)

    status: Mapped[str] = mapped_column(String(16), default="PENDING")  # PENDING/ACTIVE
    topic_prefix: Mapped[str] = mapped_column(String(255), nullable=False)

    mqtt_user: Mapped[str | None] = mapped_column(String(80), nullable=True)
    mqtt_pass: Mapped[str | None] = mapped_column(String(80), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    tenant: Mapped["Tenant"] = relationship(back_populates="devices")
    config: Mapped["DeviceConfig"] = relationship(back_populates="device", uselist=False, cascade="all, delete-orphan")

class DeviceConfig(Base):
    __tablename__ = "device_configs"
    device_id: Mapped[str] = mapped_column(ForeignKey("devices.device_id"), primary_key=True)

    # Caudales (L/h)
    flow_perm_nom: Mapped[float] = mapped_column(Float, default=0.0)
    flow_perm_min: Mapped[float] = mapped_column(Float, default=0.0)
    flow_perm_max: Mapped[float] = mapped_column(Float, default=0.0)

    flow_rej_nom: Mapped[float] = mapped_column(Float, default=0.0)
    flow_rej_min: Mapped[float] = mapped_column(Float, default=0.0)
    flow_rej_max: Mapped[float] = mapped_column(Float, default=0.0)

    # Presión intermembrana (bar)
    p_inter_nom: Mapped[float] = mapped_column(Float, default=0.0)
    p_inter_min: Mapped[float] = mapped_column(Float, default=0.0)
    p_inter_max: Mapped[float] = mapped_column(Float, default=0.0)

    # Conductividad permeado (uS/cm)
    cond_perm_nom: Mapped[float] = mapped_column(Float, default=0.0)
    cond_perm_max: Mapped[float] = mapped_column(Float, default=0.0)

    # Reglas (% y tiempos)
    cond_high_pct: Mapped[float] = mapped_column(Float, default=20.0)           # +20%
    cond_ignore_minutes: Mapped[int] = mapped_column(Integer, default=15)       # ignorar rampa inicial
    stabilize_pressure_s: Mapped[int] = mapped_column(Integer, default=120)     # estabilización 2 min
    start_wait_presostat_s: Mapped[int] = mapped_column(Integer, default=30)    # espera 30s inicio

    high_pressure_instant_pct: Mapped[float] = mapped_column(Float, default=30.0)  # +30%
    high_pressure_instant_s: Mapped[int] = mapped_column(Integer, default=20)      # sostenido 20s

    # Umbral para diferenciar 0 vs 0.2 en transductor (bar)
    p_transducer_threshold: Mapped[float] = mapped_column(Float, default=0.1)

    device: Mapped["Device"] = relationship(back_populates="config")

    __table_args__ = (
        CheckConstraint("cond_high_pct >= 0"),
        CheckConstraint("cond_ignore_minutes >= 0"),
        CheckConstraint("stabilize_pressure_s >= 0"),
        CheckConstraint("start_wait_presostat_s >= 0"),
    )
from sqlalchemy import Text

class Alarm(Base):
    __tablename__ = "alarms"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    device_id: Mapped[str] = mapped_column(ForeignKey("devices.device_id"), index=True)
    code: Mapped[str] = mapped_column(String(64), index=True)
    severity: Mapped[str] = mapped_column(String(16), default="WARN")  # INFO/WARN/CRIT
    message: Mapped[str] = mapped_column(String(255), default="")
    data_json: Mapped[str] = mapped_column(Text, default="{}")

    device: Mapped["Device"] = relationship()




class Telemetry(Base):
    from sqlalchemy import String

    mode: Mapped[str | None] = mapped_column(String(32), nullable=True)

    __tablename__ = "telemetry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    device_id: Mapped[str] = mapped_column(ForeignKey("devices.device_id"), index=True)

    # tele
    presostat_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    pressure_inter: Mapped[float | None] = mapped_column(Float, nullable=True)
    flow_perm: Mapped[float | None] = mapped_column(Float, nullable=True)
    flow_reject: Mapped[float | None] = mapped_column(Float, nullable=True)
    conductivity_perm: Mapped[float | None] = mapped_column(Float, nullable=True)
    pressure_feed: Mapped[float | None] = mapped_column(Float, nullable=True)
    temperature_feed: Mapped[float | None] = mapped_column(Float, nullable=True)

    # state
    float_call: Mapped[float | None] = mapped_column(Float, nullable=True)     # 0/1
    valve_feed: Mapped[float | None] = mapped_column(Float, nullable=True)     # 0/1
    pump_low: Mapped[float | None] = mapped_column(Float, nullable=True)       # 0/1
    pump_high: Mapped[float | None] = mapped_column(Float, nullable=True)      # 0/1


