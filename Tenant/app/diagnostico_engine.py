# app/diagnostico_engine.py
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# =========================
# Estados del motor
# =========================
S_IDLE = "IDLE"
S_STOPPED = "STOPPED"
S_PRECHECK_WAIT_PE = "PRECHECK_WAIT_PE"
S_PRECHECK_SAMPLE_PINT_OFF = "PRECHECK_SAMPLE_PINT_OFF"
S_STARTING_HIGH = "STARTING_HIGH"
S_PRODUCTION = "PRODUCTION"


# =========================
# Severidades
# =========================
SEV_INFO = "info"
SEV_WARN = "warn"
SEV_ALARM = "alarm"


# =========================
# Modelos de salida
# =========================
@dataclass
class AlarmEvent:
    code: str
    severity: str
    title: str
    detail: str = ""


@dataclass
class Recommendation:
    code: str
    title: str
    detail: str = ""


@dataclass
class EngineResult:
    severity: str = SEV_INFO
    alarms: List[AlarmEvent] = field(default_factory=list)
    recs: List[Recommendation] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


# =========================
# Samplers / utilidades
# =========================
class FixedWindowSampler:
    """
    Junta muestras por una ventana fija (en segundos) y calcula promedio, min, max.
    """
    def __init__(self, window_s: float = 30.0):
        self.window_s = float(window_s)
        self.reset()

    def reset(self):
        self.t0: Optional[float] = None
        self.samples: List[Tuple[float, float]] = []  # (ts, value)

    def add(self, ts: float, value: Optional[float]):
        if value is None:
            return
        try:
            v = float(value)
        except Exception:
            return
        if self.t0 is None:
            self.t0 = ts
        self.samples.append((ts, v))

    def elapsed(self, now: float) -> float:
        if self.t0 is None:
            return 0.0
        return max(0.0, now - self.t0)

    def done(self, now: float) -> bool:
        return self.elapsed(now) >= self.window_s

    def stats(self) -> Dict[str, Optional[float]]:
        if not self.samples:
            return {"avg": None, "min": None, "max": None, "n": 0}
        vals = [v for _, v in self.samples]
        return {
            "avg": sum(vals) / len(vals),
            "min": min(vals),
            "max": max(vals),
            "n": len(vals),
        }


class PersistLatch:
    """
    Latch para exigir persistencia: condición debe mantenerse >= persist_s.
    """
    def __init__(self):
        self.t_first_true: Optional[float] = None

    def reset(self):
        self.t_first_true = None

    def hit(self, cond: bool, now: float, persist_s: float) -> bool:
        if not cond:
            self.t_first_true = None
            return False
        if self.t_first_true is None:
            self.t_first_true = now
            return False
        return (now - self.t_first_true) >= persist_s


# =========================
# Engine
# =========================
class DiagnosticoEngine:
    """
    Motor de diagnóstico por device_id.

    Entradas esperadas (por defecto):
      - presostat_low (0/1) -> PE contacto seco (presión OK)
      - pressure_inter (float) -> Pint (transductor)
      - flow_perm (float) -> Qp
      - flow_reject (float) -> Qr
      - conductivity_perm (float) -> Ct

    Config (cfg dict) (mínimo recomendado):
      - p_inter_nom / flow_perm_nom / flow_rej_nom / cond_perm_nom (mapeados por caller a:
            ref_pint_work, ref_qp, ref_qr, ref_qsum, ref_ct)
      - start_wait_presostat_s -> t_precheck_s
      - stabilize_pressure_s -> t_stable_s
      - cond_ignore_minutes -> t_start_ignore_s
      - p_transducer_threshold -> pint_pre_zero_th
    """

    def __init__(self):
        self._st_by_dev: Dict[str, Dict[str, Any]] = {}

    # -------------------------
    # helpers para leer live
    # -------------------------
    @staticmethod
    def _live_get(live: Dict[str, Any], key: str) -> Tuple[Optional[float], Optional[float]]:
        """
        live[key] puede ser:
          - {"value": x, "ts": t}
          - x (valor crudo)
        """
        if key not in live:
            return None, None
        v = live[key]
        if isinstance(v, dict):
            return v.get("value"), v.get("ts")
        return v, None

    @staticmethod
    def _as01(x: Any) -> Optional[int]:
        if x is None:
            return None
        try:
            if isinstance(x, bool):
                return 1 if x else 0
            return 1 if int(x) != 0 else 0
        except Exception:
            return None

    @staticmethod
    def _f(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    @staticmethod
    def _pct_delta(val: Optional[float], ref: Optional[float]) -> Optional[float]:
        if val is None or ref is None or ref == 0:
            return None
        return (val - ref) / ref * 100.0

    # -------------------------
    # init state
    # -------------------------
    def _get_state(self, device_id: str) -> Dict[str, Any]:
        st = self._st_by_dev.get(device_id)
        if st is None:
            st = {
                "eng_state": S_IDLE,
                "t_cycle_start": None,
                "t_high_start": None,
                "pre_sampler": None,      # FixedWindowSampler
                "pint_off_baseline": None,
                "prod_since": None,
                "latch_A": PersistLatch(),
                "latch_B": PersistLatch(),
                "latch_C": PersistLatch(),
                "latch_D": PersistLatch(),
                "latch_E": PersistLatch(),
                "latch_F": PersistLatch(),
            }
            self._st_by_dev[device_id] = st
        return st

    # -------------------------
    # evaluate
    # -------------------------
    def evaluate(self, device_id: str, live: Dict[str, Any], cfg: Dict[str, Any]) -> EngineResult:
        now = time.time()
        res = EngineResult()

        # keys
        k_pe = cfg.get("k_pe", "presostat_low")
        k_pint = cfg.get("k_pint", "pressure_inter")
        k_qp = cfg.get("k_qp", "flow_perm")
        k_qr = cfg.get("k_qr", "flow_reject")
        k_ct = cfg.get("k_ct", "conductivity_perm")

        # refs
        ref_pint_work = self._f(cfg.get("ref_pint_work"))
        ref_qp = self._f(cfg.get("ref_qp"))
        ref_qr = self._f(cfg.get("ref_qr"))
        ref_qsum = self._f(cfg.get("ref_qsum"))
        ref_ct = self._f(cfg.get("ref_ct"))

        # tiempos
        t_precheck_s = float(cfg.get("t_precheck_s", 30) or 30)
        t_start_ignore_s = float(cfg.get("t_start_ignore_s", 60) or 60)
        t_stable_s = float(cfg.get("t_stable_s", 180) or 180)
        t_persist_s = float(cfg.get("t_persist_s", 120) or 120)
        w_avg_s = float(cfg.get("w_avg_s", 60) or 60)

        # thresholds
        pint_pre_zero_th = float(cfg.get("pint_pre_zero_th", 0.05) or 0.05)
        pint_pre_ok_th = float(cfg.get("pint_pre_ok_th", 0.25) or 0.25)

        # leer live
        pe_raw, _ = self._live_get(live, k_pe)
        pint_raw, _ = self._live_get(live, k_pint)
        qp_raw, _ = self._live_get(live, k_qp)
        qr_raw, _ = self._live_get(live, k_qr)
        ct_raw, _ = self._live_get(live, k_ct)

        pe = self._as01(pe_raw)          # 0/1
        pint = self._f(pint_raw)         # float
        qp = self._f(qp_raw)             # float
        qr = self._f(qr_raw)             # float
        ct = self._f(ct_raw)             # float

        st = self._get_state(device_id)
        eng = st.get("eng_state", S_IDLE)

        # debug base
        res.debug.update(
            {
                "state": eng,
                "pe": pe,
                "pint": pint,
                "qp": qp,
                "qr": qr,
                "ct": ct,
            }
        )

        # =========================
        # Detectar "parado" por Ct=0 (según tu criterio)
        # =========================
        if ct is not None and ct == 0:
            # si estaba corriendo, pasar a STOPPED (parada controlada)
            if eng not in (S_STOPPED, S_IDLE):
                st["eng_state"] = S_STOPPED
                st["t_cycle_start"] = None
                st["t_high_start"] = None
                st["pre_sampler"] = None
                st["prod_since"] = None
                # reset latches
                for lk in ("latch_A", "latch_B", "latch_C", "latch_D", "latch_E", "latch_F"):
                    st[lk].reset()
                res.alarms.append(
                    AlarmEvent(
                        code="I_STOP",
                        severity=SEV_INFO,
                        title="Parada detectada",
                        detail="Ct=0 → equipo detenido / fuera de agua (criterio de sensor).",
                    )
                )
            st["eng_state"] = S_STOPPED
            res.severity = max(res.severity, SEV_INFO, key=_sev_rank)
            return res

        # =========================
        # Ciclo / precheck
        # =========================
        if eng in (S_IDLE, S_STOPPED):
            # Arranca ciclo de precheck si hay actividad (Ct != 0)
            st["eng_state"] = S_PRECHECK_WAIT_PE
            st["t_cycle_start"] = now

            # sampler Pint_off con bomba baja (ventana t_precheck_s)
            st["pre_sampler"] = FixedWindowSampler(window_s=t_precheck_s)
            _toggle = st.get("pre_sampler")
            if isinstance(_toggle, FixedWindowSampler):
                _toggle.reset()

            eng = S_PRECHECK_WAIT_PE

        # Esperar PE OK
        if eng == S_PRECHECK_WAIT_PE:
            # Si presostato no OK dentro del tiempo -> E02
            t0 = st.get("t_cycle_start") or now
            if pe == 1:
                st["eng_state"] = S_PRECHECK_SAMPLE_PINT_OFF
                eng = S_PRECHECK_SAMPLE_PINT_OFF
            else:
                if (now - t0) >= t_precheck_s:
                    res.alarms.append(
                        AlarmEvent(
                            code="E02",
                            severity=SEV_ALARM,
                            title="Sin presión de entrada (PE=0)",
                            detail="El presostato de entrada no habilitó dentro del tiempo de precheck.",
                        )
                    )
                    res.severity = SEV_ALARM
                    st["eng_state"] = S_STOPPED
                    return res
                return res  # todavía esperando PE

        # Muestrear Pint_off (con bomba baja ON, alta OFF)
        if eng == S_PRECHECK_SAMPLE_PINT_OFF:
            sampler: FixedWindowSampler = st.get("pre_sampler")
            if not isinstance(sampler, FixedWindowSampler):
                sampler = FixedWindowSampler(window_s=t_precheck_s)
                st["pre_sampler"] = sampler

            sampler.add(now, pint)

            if not sampler.done(now):
                return res

            stats = sampler.stats()
            pint_off_avg = stats.get("avg")
            st["pint_off_baseline"] = pint_off_avg

            # Diagnóstico de precheck:
            # - Pint ~0 => bomba baja no mueve agua / cisterna sin agua / caño vacío (tu criterio)
            if pint_off_avg is None or pint_off_avg <= pint_pre_zero_th:
                res.alarms.append(
                    AlarmEvent(
                        code="E01",
                        severity=SEV_ALARM,
                        title="Cisterna sin agua o bomba baja sin caudal",
                        detail=f"Pint_off promedio={pint_off_avg}. Umbral={pint_pre_zero_th}.",
                    )
                )
                res.severity = SEV_ALARM
                st["eng_state"] = S_STOPPED
                return res

            # - Si cae respecto a baseline/nominal: warning filtro sedimento ensuciándose
            #   Si no hay ref, no emitimos.
            if ref_pint_work is not None:
                # Esto es un proxy simple: si pint_off < ref_pint_work*0.95 -> W10
                # (ajustable; podés usar otro ref en cfg si querés)
                if pint_off_avg < (ref_pint_work * 0.95):
                    res.alarms.append(
                        AlarmEvent(
                            code="W10",
                            severity=SEV_WARN,
                            title="Prefiltros ensuciándose (Pint_off baja)",
                            detail=f"Pint_off avg={pint_off_avg:.3f} < 95% de ref_pint_work={ref_pint_work:.3f}.",
                        )
                    )
                    res.severity = SEV_WARN

            # Pasar a producción (asumimos alta ON y empezará a llegar Qp/Qr/Pint_work)
            st["eng_state"] = S_PRODUCTION
            st["prod_since"] = now
            eng = S_PRODUCTION

            # reset latches al entrar a producción
            for lk in ("latch_A", "latch_B", "latch_C", "latch_D", "latch_E", "latch_F"):
                st[lk].reset()

        # =========================
        # Producción / reglas principales
        # =========================
        if eng == S_PRODUCTION:
            t_prod0 = st.get("prod_since") or now
            t_in_prod = now - t_prod0

            # ignorar conductividad al inicio (estabilización)
            ct_valid = True
            if t_in_prod < t_start_ignore_s:
                ct_valid = False

            # helpers
            qsum = None
            if qp is not None and qr is not None:
                qsum = qp + qr

            # comparaciones con referencias
            qp_low = (qp is not None and ref_qp is not None and qp < (ref_qp * 0.90))
            qp_very_low = (qp is not None and ref_qp is not None and qp < (ref_qp * 0.10))

            qr_low = (qr is not None and ref_qr is not None and qr < (ref_qr * 0.70))
            qr_high = (qr is not None and ref_qr is not None and qr > (ref_qr * 1.20))

            qsum_low = (qsum is not None and ref_qsum is not None and qsum < (ref_qsum * 0.90))
            qsum_ok = (qsum is not None and ref_qsum is not None and qsum >= (ref_qsum * 0.95))

            pint_high = (pint is not None and ref_pint_work is not None and pint > (ref_pint_work * 1.07))
            pint_low = (pint is not None and ref_pint_work is not None and pint < (ref_pint_work * 0.70))
            pint_ok = (pint is not None and ref_pint_work is not None and (ref_pint_work * 0.90) <= pint <= (ref_pint_work * 1.07))

            # Conductividad alta vs setpoint
            ct_high = False
            if ct_valid and ct is not None and ref_ct is not None:
                # por defecto: +20%
                pct = float(cfg.get("cond_high_pct", 20.0) or 20.0)
                ct_high = ct > (ref_ct * (1.0 + pct / 100.0))

            # -------------------------
            # Reglas derivadas de tu lista
            # -------------------------
            # A) Flush roto / aguja mal calibrada:
            #    rechazo alto, permeado bajo/0, conductividad nula (o muy baja)
            cond_A = False
            if (qr_high or (qr is not None and ref_qr is None and qr > 0)) and (qp_very_low or (qp is not None and qp <= 0.05)):
                # ct "nula" criterio: ==0 o muy baja
                if ct is not None and ct <= 1.0:
                    cond_A = True

            if st["latch_A"].hit(cond_A, now, t_persist_s):
                res.alarms.append(
                    AlarmEvent(
                        code="A_FLUSH_BYPASS",
                        severity=SEV_WARN,
                        title="Flush abierto / bypass (posible válvula flush rota o aguja mal calibrada)",
                        detail="Qr alto + Qp muy bajo + Ct casi nula.",
                    )
                )
                res.recs.append(
                    Recommendation(
                        code="A_REC",
                        title="Verificar flush y restrictor",
                        detail="Revisar válvula de flush, estado del check y calibración de aguja/restrictor de rechazo.",
                    )
                )
                res.severity = _max_sev(res.severity, SEV_WARN)

            # B) O-ring / membrana averiada:
            #    permeado alto, rechazo normal/bajo, conductividad alta
            cond_B = False
            if ct_high and (qp is not None and ref_qp is not None and qp > (ref_qp * 1.05)) and (qr_low or (qr is not None and ref_qr is not None and qr <= ref_qr)):
                cond_B = True

            if st["latch_B"].hit(cond_B, now, t_persist_s):
                res.alarms.append(
                    AlarmEvent(
                        code="B_ORING_MEM",
                        severity=SEV_ALARM,
                        title="Conductividad alta con permeado alto (posible O-ring/membrana dañada)",
                        detail="Qp alto + Qr normal/bajo + Ct por encima del límite.",
                    )
                )
                res.recs.append(
                    Recommendation(
                        code="B_REC",
                        title="Revisar integridad de membrana y O-rings",
                        detail="Inspeccionar O-rings, portamembrana y membrana por daños o bypass interno.",
                    )
                )
                res.severity = _max_sev(res.severity, SEV_ALARM)

            # C) Aire / poca agua:
            #    Pint baja + rechazo alto + permeado bajo (Ct sin importancia)
            cond_C = False
            if pint_low and (qr_high or (qr is not None and ref_qr is None and qr > 0)) and (qp_low or qp_very_low):
                cond_C = True

            if st["latch_C"].hit(cond_C, now, t_persist_s):
                res.alarms.append(
                    AlarmEvent(
                        code="C_AIRE_AGUA",
                        severity=SEV_WARN,
                        title="Pint baja con caudales anómalos (posible aire en cañería / poca agua)",
                        detail="Pint baja + Qr alto + Qp bajo.",
                    )
                )
                res.recs.append(
                    Recommendation(
                        code="C_REC",
                        title="Purgar y verificar alimentación",
                        detail="Verificar succión, entrada de aire, tanque/cisterna y purga del sistema.",
                    )
                )
                res.severity = _max_sev(res.severity, SEV_WARN)

            # D) Aguja muy cerrada / obstrucción en rechazo:
            #    Pint alta + rechazo bajo + permeado alto
            cond_D = False
            if pint_high and (qr_low or (qr is not None and ref_qr is not None and qr < (ref_qr * 0.60))) and (qp is not None and ref_qp is not None and qp > (ref_qp * 1.05)):
                cond_D = True

            if st["latch_D"].hit(cond_D, now, t_persist_s):
                res.alarms.append(
                    AlarmEvent(
                        code="D_RECH_OBS",
                        severity=SEV_WARN,
                        title="Pint alta con rechazo bajo (posible obstrucción en rechazo / aguja cerrada)",
                        detail="Pint alta + Qr bajo + Qp alto.",
                    )
                )
                res.recs.append(
                    Recommendation(
                        code="D_REC",
                        title="Revisar línea de rechazo y aguja",
                        detail="Verificar restrictor/aguja, válvula, manguera de rechazo, posibles tapones.",
                    )
                )
                res.severity = _max_sev(res.severity, SEV_WARN)

            # E) Membrana obstruida (requiere limpieza/cambio):
            #    Pint alta + Qr normal/bajo + Qp normal/bajo (Ct sin importancia)
            cond_E = False
            if pint_high and (qr_low or (qr is not None and ref_qr is not None and qr <= ref_qr)) and (qp is not None and ref_qp is not None and qp <= (ref_qp * 1.00)):
                cond_E = True

            if st["latch_E"].hit(cond_E, now, t_persist_s):
                res.alarms.append(
                    AlarmEvent(
                        code="E_MEM_OBS",
                        severity=SEV_WARN,
                        title="Pint alta con caudales contenidos (posible ensuciamiento/incrustación en membrana)",
                        detail="Pint alta + Qp normal/bajo + Qr normal/bajo.",
                    )
                )
                res.recs.append(
                    Recommendation(
                        code="E_REC",
                        title="Evaluar limpieza química / recambio",
                        detail="Revisar SDI, prefiltrado y considerar CIP / cambio de membrana según historial.",
                    )
                )
                res.severity = _max_sev(res.severity, SEV_WARN)

            # F) Conductividad alta con variables normales:
            #    Pint normal + caudales normales + Ct arriba del setpoint
            cond_F = False
            if ct_high and pint_ok and (not qsum_low) and qsum_ok:
                cond_F = True

            if st["latch_F"].hit(cond_F, now, t_persist_s):
                res.alarms.append(
                    AlarmEvent(
                        code="F_CT_HIGH",
                        severity=SEV_WARN,
                        title="Conductividad alta con operación normal (ensuciamiento/deterioro progresivo)",
                        detail="Pint y caudales en rango pero Ct supera el límite configurado.",
                    )
                )
                res.recs.append(
                    Recommendation(
                        code="F_REC",
                        title="Monitorear tendencia y programar mantenimiento",
                        detail="Comparar contra historial y baseline; considerar lavado y revisión de membrana.",
                    )
                )
                res.severity = _max_sev(res.severity, SEV_WARN)

            # debug adicional
            res.debug.update(
                {
                    "t_in_prod_s": round(t_in_prod, 1),
                    "ct_valid": ct_valid,
                    "qsum": qsum,
                    "pint_high": pint_high,
                    "pint_low": pint_low,
                    "qp_low": qp_low,
                    "qr_low": qr_low,
                    "ct_high": ct_high,
                }
            )

            return res

        return res


# =========================
# Helpers severidad
# =========================
def _sev_rank(s: str) -> int:
    return {SEV_INFO: 0, SEV_WARN: 1, SEV_ALARM: 2}.get(s, 0)


def _max_sev(a: str, b: str) -> str:
    return b if _sev_rank(b) > _sev_rank(a) else a
