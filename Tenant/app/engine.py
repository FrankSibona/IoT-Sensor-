import time
from dataclasses import dataclass, field
from sqlalchemy.orm import Session

from .db import SessionLocal
from .models import DeviceConfig
from . import crud

@dataclass
class DeviceContext:
    mode: str = "IDLE"                 # IDLE/STARTING/PRODUCTION/LOCKOUT
    start_ts: float | None = None      # inicio de sesión (float_call_start)
    last_eval_ts: float = 0.0

    retries: int = 0
    retry_last_ts: float = 0.0

    # cache de últimos valores
    tele: dict = field(default_factory=dict)    # presostat_low, pressure_inter, flows, cond...
    state: dict = field(default_factory=dict)   # float_call, pumps, valve_feed, mode (informativo)

    # flags de rampa/estabilización
    pressure_stable_ts: float | None = None
    cond_ok_to_eval_ts: float | None = None

    # antirepetición de alarmas
    last_alarm_ts: dict = field(default_factory=dict)  # code -> ts

class DiagnosticEngine:
    def __init__(self):
        self.ctx: dict[str, DeviceContext] = {}

    # ---------- util ----------
    def _get_cfg(self, db: Session, device_id: str) -> DeviceConfig | None:
        return db.get(DeviceConfig, device_id)

    def _now(self) -> float:
        return time.time()

    def _cooldown_ok(self, c: DeviceContext, code: str, cooldown_s: int = 120) -> bool:
        t = c.last_alarm_ts.get(code, 0.0)
        return (self._now() - t) >= cooldown_s

    def _raise_alarm(self, device_id: str, c: DeviceContext, code: str, severity: str, message: str, data: dict, cooldown_s: int = 120):
        if not self._cooldown_ok(c, code, cooldown_s):
            return
        db: Session = SessionLocal()
        try:
            crud.create_alarm(db, device_id, code, severity, message, data)
            c.last_alarm_ts[code] = self._now()
        finally:
            db.close()

    # ---------- ingreso de muestras ----------
    def ingest(self, device_id: str, tele: dict | None = None, state: dict | None = None, event: dict | None = None):
        c = self.ctx.setdefault(device_id, DeviceContext())
        if tele:
            c.tele.update(tele)
        if state:
            c.state.update(state)

        # eventos (si los usás)
        if event and event.get("name") == "float_call_start":
            self._start_session(c)
        if event and event.get("name") == "tank_full":
            self._end_session(c)

        # si no hay evento, detectá sesión por estado float_call
        float_call = int(c.state.get("float_call", 0) or 0)
        if float_call == 1 and c.mode == "IDLE":
            self._start_session(c)
        if float_call == 0 and c.mode in ("STARTING", "PRODUCTION"):
            # flotante dejó de pedir -> fin
            self._end_session(c)

        # evaluá reglas
        self.evaluate(device_id)

    def _start_session(self, c: DeviceContext):
        now = self._now()
        c.mode = "STARTING"
        c.start_ts = now
        c.retries = 0
        c.retry_last_ts = 0.0
        c.pressure_stable_ts = now + 0  # se setea en base a cfg al evaluar
        c.cond_ok_to_eval_ts = now + 0

    def _end_session(self, c: DeviceContext):
        c.mode = "IDLE"
        c.start_ts = None
        c.pressure_stable_ts = None
        c.cond_ok_to_eval_ts = None
        c.retries = 0
        c.retry_last_ts = 0.0

    # ---------- evaluación ----------
    def evaluate(self, device_id: str):
        c = self.ctx.get(device_id)
        if not c:
            return

        now = self._now()
        # no evalúes demasiado seguido (evita spam)
        if now - c.last_eval_ts < 1.0:
            return
        c.last_eval_ts = now

        db: Session = SessionLocal()
        try:
            cfg = self._get_cfg(db, device_id)
            if not cfg:
                return

            # set gates de tiempo una sola vez por sesión
            if c.start_ts and c.pressure_stable_ts is None:
                c.pressure_stable_ts = c.start_ts + cfg.stabilize_pressure_s
            if c.start_ts and c.cond_ok_to_eval_ts is None:
                c.cond_ok_to_eval_ts = c.start_ts + (cfg.cond_ignore_minutes * 60)

            # valores actuales (con defaults)
            presostat_low = int(c.tele.get("presostat_low", 0) or 0)
            p_inter = float(c.tele.get("pressure_inter", 0.0) or 0.0)
            flow_perm = float(c.tele.get("flow_perm", 0.0) or 0.0)
            flow_rej = float(c.tele.get("flow_reject", 0.0) or 0.0)
            cond_perm = float(c.tele.get("conductivity_perm", 0.0) or 0.0)

            valve_feed = int(c.state.get("valve_feed", 0) or 0)
            pump_low = int(c.state.get("pump_low", 0) or 0)
            pump_high = int(c.state.get("pump_high", 0) or 0)
            float_call = int(c.state.get("float_call", 0) or 0)

            # coherencia mínima: si no pide agua, no diagnostiques
            if float_call == 0:
                return

            # --------- regla instantánea: presión demasiado alta desde el arranque ---------
            if c.start_ts:
                high_thr = cfg.p_inter_nom * (1.0 + cfg.high_pressure_instant_pct / 100.0) if cfg.p_inter_nom > 0 else 0
                if high_thr > 0 and p_inter >= high_thr:
                    # requiere sostenido cfg.high_pressure_instant_s; simple: si ya pasaron esos segundos desde start y sigue alto
                    if (now - c.start_ts) >= cfg.high_pressure_instant_s:
                        self._raise_alarm(
                            device_id, c,
                            code="PRESSURE_HIGH_INSTANT",
                            severity="WARN",
                            message="Presión intermembrana alta sostenida al inicio",
                            data={"p_inter": p_inter, "threshold": high_thr},
                            cooldown_s=180,
                        )

            # --------- estado STARTING: 3 reintentos + subrutina ---------
            if c.mode == "STARTING":
                # esperar ventana de inicio
                if c.start_ts and (now - c.start_ts) < cfg.start_wait_presostat_s:
                    return

                # si presostato OK -> producción
                if presostat_low == 1:
                    c.mode = "PRODUCTION"
                    return

                # presostato NO OK
                # política de reintento: cada 30s (podés ajustar luego)
                retry_cooldown = 30
                if (now - c.retry_last_ts) < retry_cooldown:
                    return

                c.retries += 1
                c.retry_last_ts = now

                if c.retries < 3:
                    self._raise_alarm(
                        device_id, c,
                        code="START_RETRY",
                        severity="INFO",
                        message=f"Reintento de arranque ({c.retries}/3) por falta de presión en presostato",
                        data={"retries": c.retries, "presostat_low": presostat_low},
                        cooldown_s=5,
                    )
                    return

                # 3er fallo -> subrutina: transductor > 0.1 => filtro sucio; si 0 => no hay agua
                if p_inter > cfg.p_transducer_threshold:
                    self._raise_alarm(
                        device_id, c,
                        code="SEDIMENT_FILTER_DIRTY",
                        severity="WARN",
                        message="Posible filtro de sedimento sucio (hay presión en transductor, presostato no cierra)",
                        data={"presostat_low": presostat_low, "pressure_inter": p_inter, "threshold": cfg.p_transducer_threshold},
                        cooldown_s=300,
                    )
                else:
                    self._raise_alarm(
                        device_id, c,
                        code="NO_WATER_FEED",
                        severity="CRIT",
                        message="No hay alimentación de agua (sin presión en presostato y transductor)",
                        data={"presostat_low": presostat_low, "pressure_inter": p_inter, "threshold": cfg.p_transducer_threshold},
                        cooldown_s=300,
                    )

                # lockout simple: no sigas spameando en STARTING
                c.mode = "LOCKOUT"
                return

            # --------- PRODUCCIÓN: solo si está todo “en marcha” ---------
            if c.mode == "PRODUCTION":
                if not (valve_feed == 1 and pump_low == 1 and pump_high == 1):
                    # si alguien apagó algo, no diagnostiques como si estuviera produciendo
                    return

                # gate de estabilización de presión
                if c.pressure_stable_ts and now < c.pressure_stable_ts:
                    return

                # reglas por rangos configurados
                rej_normal = (cfg.flow_rej_min <= flow_rej <= cfg.flow_rej_max) if cfg.flow_rej_max > 0 else True
                perm_low = (flow_perm < cfg.flow_perm_min) if cfg.flow_perm_min > 0 else False
                perm_normal = (cfg.flow_perm_min <= flow_perm <= cfg.flow_perm_max) if cfg.flow_perm_max > 0 else True

                # 1) Ensuciamiento membrana:
                # presostato OK, presión intermembrana muy alta, permeado bajo, rechazo normal
                high_thr = cfg.p_inter_nom * 1.30 if cfg.p_inter_nom > 0 else 0
                if presostat_low == 1 and high_thr > 0 and p_inter >= high_thr and perm_low and rej_normal:
                    self._raise_alarm(
                        device_id, c,
                        code="MEMBRANE_FOULING",
                        severity="WARN",
                        message="Alerta por posible ensuciamiento de membrana (P alta + permeado bajo)",
                        data={"p_inter": p_inter, "p_thr": high_thr, "flow_perm": flow_perm, "flow_reject": flow_rej},
                        cooldown_s=300,
                    )

                # 2) Flush/aguja:
                # presostato OK, presión inter muy baja, rechazo alto, permeado bajo
                low_thr = cfg.p_inter_nom * 0.70 if cfg.p_inter_nom > 0 else 0
                rej_high = (flow_rej > cfg.flow_rej_max) if cfg.flow_rej_max > 0 else False
                if presostat_low == 1 and low_thr > 0 and p_inter <= low_thr and perm_low and rej_high:
                    self._raise_alarm(
                        device_id, c,
                        code="FLUSH_VALVE_OR_NEEDLE",
                        severity="WARN",
                        message="Posible falla EV flush o mala regulación de aguja (P baja + rechazo alto + permeado bajo)",
                        data={"p_inter": p_inter, "p_thr": low_thr, "flow_perm": flow_perm, "flow_reject": flow_rej},
                        cooldown_s=300,
                    )

                # 3) Conductividad: por ahora solo “alta instantánea” después de rampa
                # (lo de +20% por 30 min lo hacemos con acumulador en el próximo paso)
                if c.cond_ok_to_eval_ts and now >= c.cond_ok_to_eval_ts:
                    if cfg.cond_perm_max > 0 and cond_perm > cfg.cond_perm_max and perm_normal and rej_normal:
                        self._raise_alarm(
                            device_id, c,
                            code="PERMEATE_COND_HIGH",
                            severity="WARN",
                            message="Conductividad de permeado alta (posible bypass/oring o membrana)",
                            data={"cond_perm": cond_perm, "cond_max": cfg.cond_perm_max},
                            cooldown_s=300,
                        )

        finally:
            db.close()

engine = DiagnosticEngine()
