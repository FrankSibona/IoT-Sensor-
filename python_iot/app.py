"""
Fyntek RO Backend - worker.py v3.3
====================================

NUEVO en v3.4 — Separación Telemetría vs KPIs:

  KPIEngine refactorizado en dos capas:

  read_physical()
    → Nueva capa de telemetría cruda (siempre activa)
    → Variables físicas medidas directamente del proceso:
       - caudal (permeado / rechazo)
       - presiones
       - volúmenes acumulados
    → Se calculan SIEMPRE, independientemente del estado operativo

  compute_kpis()
    → KPIs derivados (interpretación del proceso)
    → Solo se calculan cuando el sistema está en PRODUCING
    → Evita generar métricas sin contexto (ruido matemático)

IMPACTO FUNCIONAL:

  Telemetría física persistente
    → flow_perm_lpm y pressure_membrane ya NO desaparecen en IDLE
    → El dashboard refleja variables reales incluso fuera de producción
    → Mejora directa en credibilidad del sistema

  KPIs condicionados por operación
    → recovery, efficiency, cost_per_liter solo existen en producción
    → Se elimina ambigüedad en períodos sin flujo

  device_status más representativo
    → Siempre contiene la última lectura física válida
    → Independiente del estado del equipo

  Consistencia con el modelo físico
    → Se separa explícitamente:
        medición (realidad)
        vs
        interpretación (análisis)

NO_FLOW_DETECTED (mejora implícita)
  → Sigue operando correctamente usando datos físicos directos
  → Detecta flujo nulo incluso fuera de métricas calculadas

ARQUITECTURA DE CAPAS (actualizada):

  Capa 0 → Telemetría física (SIEMPRE activa)
           → sensores, señales crudas, realidad del proceso

  Capa 1 → Eventos (instantáneos)
           → alertas inmediatas (ej: NO_FLOW_DETECTED)

  Capa 2 → Diagnóstico (con histéresis)
           → problemas confirmados en el tiempo

  Capa 3 → Tendencias (informativas)
           → análisis de comportamiento (no alertan)

  Capa 4 → Negocio (cada 5 min)
           → impacto económico y operativo

CAMBIO CLAVE DE FILOSOFÍA:

  Antes:
    "El sistema muestra lo que el estado permite calcular"

  Ahora:
    "El sistema muestra lo que realmente está pasando,
     y calcula KPIs solo cuando tiene sentido hacerlo"

  → Esto elimina falsos vacíos de datos
  → y evita interpretaciones incorrectas del cliente

NUEVO en v3.3 — Métricas de negocio:

  BusinessMetricsEngine
    → Responde las 3 preguntas que le importan al cliente:
      1. ¿Estoy produciendo lo que debería?   → fulfillment_pct
      2. ¿Estoy desperdiciando agua/energía?  → waste_pct, waste_liters
      3. ¿Mi equipo se está degradando?       → degradation_pct, degradation_label

  RiskEngine
    → Riesgo operativo: LOW | MEDIUM | HIGH | CRITICAL
    → Basado en diagnóstico actual + tendencias, no en variables aisladas

  DegradationTracker
    → "Tu sistema perdió 8.3% de rendimiento en 7 días"
    → Compara eficiencia actual vs baseline o vs hace N días
    → Se calcula cada 5 minutos por dispositivo (no por mensaje)

  Staleness indicator
    → health_age_hours: cuántas horas tiene el último diagnóstico de salud
    → Evita vender certeza donde hay incertidumbre

ARQUITECTURA DE CAPAS:
  Capa 1 → Eventos (instantáneos)        → alerta inmediata
  Capa 2 → Diagnóstico (con histéresis)  → confirmado en 60s
  Capa 3 → Tendencias (informativas)     → nunca alertan
  Capa 4 → Negocio (cada 5 min)          → KPIs para cliente final
"""

import json
import logging
import os
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timezone, date
from typing import Optional, Dict, List, Any

import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.pool
import requests
from flask import Flask, request, jsonify

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/var/log/ro_backend.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ro_backend")

# ============================================================
# CONFIGURACIÓN
# ============================================================

DB_HOST = os.getenv("DB_HOST", "ro-postgres")
DB_NAME = os.getenv("DB_NAME", "iot_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

MQTT_BROKER = os.getenv("MQTT_BROKER", "ro-mosquitto")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER   = os.getenv("MQTT_USER", "kairox")
MQTT_PASS   = os.getenv("MQTT_PASS", "admin0102")
MQTT_TOPIC  = "fyntek/#"

TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_ADMIN_CHAT = os.getenv("TELEGRAM_ADMIN_CHAT", "")

THRESHOLDS = {
    "pressure_max_bar":           float(os.getenv("THRESH_PRESSURE_MAX",   "9.0")),
    "pressure_low_bar":           float(os.getenv("THRESH_PRESSURE_LOW",   "2.0")),
    "efficiency_warning":         float(os.getenv("THRESH_EFF_WARNING",    "0.85")),
    "efficiency_critical":        float(os.getenv("THRESH_EFF_CRITICAL",   "0.70")),
    "recovery_min":               float(os.getenv("THRESH_RECOVERY_MIN",   "0.25")),
    "recovery_max":               float(os.getenv("THRESH_RECOVERY_MAX",   "0.85")),
    "flow_perm_min_lpm":          float(os.getenv("THRESH_FLOW_MIN",       "0.3")),
    "alert_cooldown_sec":         int(os.getenv("ALERT_COOLDOWN",          "300")),
    "trend_window":               int(os.getenv("TREND_WINDOW",            "30")),
    "pressure_trend_threshold":   float(os.getenv("TREND_PRESSURE",        "0.02")),
    "efficiency_trend_threshold": float(os.getenv("TREND_EFFICIENCY",      "-0.005")),
    "hysteresis_confirm_sec":     int(os.getenv("HYSTERESIS_CONFIRM",      "60")),
    "hysteresis_clear_sec":       int(os.getenv("HYSTERESIS_CLEAR",        "120")),
    "no_flow_timeout_sec":        int(os.getenv("NO_FLOW_TIMEOUT",         "30")),
    # Cada cuántos segundos se recalculan métricas de negocio por dispositivo
    "biz_refresh_sec":            int(os.getenv("BIZ_REFRESH",             "300")),
    # Ventana de días para calcular tendencia de degradación
    "degradation_window_days":    int(os.getenv("DEGRADATION_DAYS",        "7")),
    # Mínima pérdida de eficiencia para reportar degradación (%)
    "degradation_min_pct":        float(os.getenv("DEGRADATION_MIN_PCT",   "3.0")),
}

BASELINE_FIELDS = [
    "efficiency_warn_low", "efficiency_crit_low",
    "recovery_warn_low",   "recovery_warn_high",
    "flow_perm_warn_low",  "pressure_warn_high",
    "pressure_crit_high",  "delta_pressure_warn_high",
]

DIAG_SCORES = {
    "FAULT_NO_WATER":       95,
    "FAULT_SYSTEM":        100,
    "NO_RAW_WATER":         90,
    "HIGH_PRESSURE":        90,
    "NO_FLOW_DETECTED":     88,
    "CRITICAL_EFFICIENCY":  80,
    "MEMBRANE_DEGRADED":    75,
    "MEMBRANE_FOULING":     70,
    "MEMBRANE_SCALING":     65,
    "PROGRESSIVE_FOULING":  60,
    "LOW_RECOVERY":         45,
    "LOW_EFFICIENCY":       40,
    "LOW_PERMEATE_FLOW":    35,
    "LOW_PRESSURE":         35,
    "DECLINING_EFFICIENCY": 30,
}

IMMEDIATE_ALERT_CODES = {
    "FAULT_NO_WATER", "FAULT_SYSTEM",
    "NO_RAW_WATER", "HIGH_PRESSURE",
    "CRITICAL_EFFICIENCY", "NO_FLOW_DETECTED",
}

ACTIVE_STATES  = {"PRODUCING", "STARTING"}
PASSIVE_STATES = {"IDLE", "STOPPING", "FLUSHING"}

# Pesos de riesgo por diagnóstico
RISK_WEIGHTS = {
    "FAULT_SYSTEM":         100,
    "FAULT_NO_WATER":       95,
    "NO_RAW_WATER":         90,
    "HIGH_PRESSURE":        85,
    "NO_FLOW_DETECTED":     80,
    "CRITICAL_EFFICIENCY":  75,
    "MEMBRANE_DEGRADED":    60,
    "MEMBRANE_FOULING":     55,
    "MEMBRANE_SCALING":     50,
    "LOW_EFFICIENCY":       30,
    "LOW_RECOVERY":         25,
    "LOW_PERMEATE_FLOW":    20,
    "LOW_PRESSURE":         20,
    "FOULING_PROGRESSIVE":  35,  # tendencia
    "DECLINING_EFFICIENCY": 25,  # tendencia
}

def map_severity_to_health(severity: str) -> str:
    return {"OK": "HEALTHY", "WARNING": "WARNING", "CRITICAL": "CRITICAL"}.get(severity, "UNKNOWN")

# ============================================================
# DATABASE POOL
# ============================================================

class DatabasePool:
    def __init__(self):
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

    def connect(self):
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2, maxconn=10,
            host=DB_HOST, port=DB_PORT,
            database=DB_NAME, user=DB_USER, password=DB_PASS,
        )
        log.info("✅ DB pool conectado")

    def execute(self, sql: str, params: tuple = ()):
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error(f"DB error: {e} | SQL: {sql[:80]}")
        finally:
            self._pool.putconn(conn)

    def fetchall(self, sql: str, params: tuple = ()):
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        except Exception as e:
            log.error(f"DB fetch error: {e}")
            return []
        finally:
            self._pool.putconn(conn)

    def insert_returning(self, sql: str, params: tuple = ()):
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
        except Exception as e:
            conn.rollback()
            log.error(f"DB insert_returning error: {e}")
            return None
        finally:
            self._pool.putconn(conn)


db = DatabasePool()

# ============================================================
# UTILIDADES
# ============================================================

STATE_MAP = {
    "IDLE": 0, "STARTING": 1, "PRODUCING": 2,
    "FLUSHING": 3, "STOPPING": 4, "FAULT": 5,
}

def ts_to_utc(ts_unix: Any) -> Optional[datetime]:
    try:
        val = int(ts_unix)
        if val <= 0:
            return None
        return datetime.fromtimestamp(val, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None

def validate_float(value: Any, vmin: float = -1000, vmax: float = 1000) -> Optional[float]:
    try:
        v = float(value)
        return v if vmin <= v <= vmax else None
    except (TypeError, ValueError):
        return None

def validate_bool(value: Any) -> Optional[bool]:
    try:
        return value if isinstance(value, bool) else bool(int(value))
    except (TypeError, ValueError):
        return None

# ============================================================
# BASELINE CACHE
# ============================================================

class BaselineCache:
    _FALLBACK = {
        "efficiency_warn_low":      "efficiency_warning",
        "efficiency_crit_low":      "efficiency_critical",
        "recovery_warn_low":        "recovery_min",
        "recovery_warn_high":       "recovery_max",
        "flow_perm_warn_low":       "flow_perm_min_lpm",
        "pressure_warn_high":       "pressure_max_bar",
        "pressure_crit_high":       "pressure_max_bar",
        "delta_pressure_warn_high": "pressure_max_bar",
    }
    _cache: Dict[str, Dict] = {}

    @classmethod
    def get(cls, device_id: str) -> Dict:
        cached = cls._cache.get(device_id, {})
        if cached.get("_ts", 0) > time.time() - 600:
            return cached["data"]

        cols = []
        for field in BASELINE_FIELDS:
            cols += [f"{field}_learned", f"{field}_manual", f"{field}_source"]

        rows = db.fetchall(
            f"SELECT {', '.join(cols)} FROM device_baseline WHERE device_id = %s",
            (device_id,)
        )
        resolved = {}
        if rows:
            row = rows[0]
            for i, field in enumerate(BASELINE_FIELDS):
                learned = row[i * 3]
                manual  = row[i * 3 + 1]
                source  = row[i * 3 + 2] or "learned"
                if source == "manual" and manual is not None:
                    resolved[field] = manual
                elif learned is not None:
                    resolved[field] = learned
                else:
                    resolved[field] = THRESHOLDS[cls._FALLBACK[field]]
        else:
            for field in BASELINE_FIELDS:
                resolved[field] = THRESHOLDS[cls._FALLBACK[field]]

        cls._cache[device_id] = {"data": resolved, "_ts": time.time()}
        return resolved

    @classmethod
    def get_efficiency_baseline(cls, device_id: str) -> Optional[float]:
        """Retorna la eficiencia media aprendida, o None si no hay baseline."""
        rows = db.fetchall(
            "SELECT efficiency_mean FROM device_baseline WHERE device_id = %s",
            (device_id,)
        )
        return rows[0][0] if rows and rows[0][0] else None

    @classmethod
    def invalidate(cls, device_id: str):
        cls._cache.pop(device_id, None)

# ============================================================
# KPI ENGINE
# ============================================================

class KPIEngine:
    _last_quality:  Dict[str, Dict] = {}
    _device_config: Dict[str, Dict] = {}

    @classmethod
    def update_quality_cache(cls, device_id: str, quality_data: Dict):
        cls._last_quality[device_id] = quality_data

    @classmethod
    def _get_config(cls, device_id: str) -> Dict:
        cached = cls._device_config.get(device_id, {})
        if cached.get("_ts", 0) > time.time() - 300:
            return cached
        rows = db.fetchall(
            "SELECT pump_power_kw, cost_kwh, cost_water_m3, daily_target_liters "
            "FROM device_config WHERE device_id = %s",
            (device_id,)
        )
        config = {
            "pump_power_kw":       rows[0][0] if rows else 0.75,
            "cost_kwh":            rows[0][1] if rows else 0.12,
            "cost_water_m3":       rows[0][2] if rows else 0.80,
            "daily_target_liters": rows[0][3] if rows else 0.0,
            "_ts":                 time.time(),
        }
        cls._device_config[device_id] = config
        return config

    @classmethod
    def invalidate_config(cls, device_id: str):
        cls._device_config.pop(device_id, None)

    @classmethod
    def read_physical(cls, process: Dict) -> Dict:
        """
        Extrae variables físicas medidas.
        Se llama SIEMPRE, independientemente del estado operativo.
        Estas son telemetría, no interpretación.
        """
        return {
            "flow_perm_lpm":        validate_float(process.get("flow_perm_lpm"),          0, 100),
            "flow_rechazo_lpm":     validate_float(process.get("flow_rechazo_lpm"),        0, 100),
            "pressure_membrane_bar": validate_float(process.get("pressure_membrane_bar"),  0, 50),
            "pressure_brine_bar":   validate_float(process.get("pressure_brine_bar"),      0, 50),
            "volume_perm_l":        validate_float(process.get("volume_perm_l"),           0, 1e7),
            "volume_rechazo_l":     validate_float(process.get("volume_rechazo_l"),        0, 1e7),
        }

    @classmethod
    def compute_kpis(cls, device_id: str, physical: Dict, state: str) -> Optional[Dict]:
        """
        Calcula KPIs derivados.
        Solo tiene sentido cuando el equipo está produciendo.
        Fuera de PRODUCING los ratios son ruido matemático.
        """
        if state not in ACTIVE_STATES:
            return None

        flow_p  = physical.get("flow_perm_lpm")
        flow_r  = physical.get("flow_rechazo_lpm")
        p_mem   = physical.get("pressure_membrane_bar")
        p_brine = physical.get("pressure_brine_bar")

        if flow_p is None or flow_r is None:
            return None

        total_flow      = flow_p + flow_r
        recovery        = (flow_p / total_flow) if total_flow > 0.01 else None
        rejection_ratio = (flow_r / flow_p)     if flow_p > 0.01    else None
        delta_p         = (p_mem - p_brine)      if p_mem and p_brine else None

        quality    = cls._last_quality.get(device_id, {})
        tds_in     = validate_float(quality.get("tds_in_raw"),  0, 5)
        tds_out    = validate_float(quality.get("tds_out_raw"), 0, 5)
        efficiency = None
        if tds_in and tds_in > 0.05:
            efficiency = 1.0 - (tds_out / tds_in) if tds_out is not None else None

        config         = cls._get_config(device_id)
        cost_per_liter = None
        if flow_p and flow_p > 0.01 and total_flow > 0.01:
            cost_energy    = config["pump_power_kw"] * (config["cost_kwh"] / 60)
            cost_water     = (total_flow / 1000) * config["cost_water_m3"] * (1000 / 60)
            cost_per_liter = (cost_energy + cost_water) / flow_p

        return {
            "recovery":           recovery,
            "efficiency":         efficiency,
            "rejection_ratio":    rejection_ratio,
            "delta_pressure_bar": delta_p,
            "flow_perm_lpm":      flow_p,
            "flow_rechazo_lpm":   flow_r,
            "tds_in_raw":         tds_in,
            "tds_out_raw":        tds_out,
            "cost_per_liter":     cost_per_liter,
        }

# ============================================================
# TREND ANALYZER
# ============================================================

class TrendAnalyzer:
    def __init__(self, window: int = 30):
        self.window = window
        self._buffers: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: {
                "pressure":   deque(maxlen=window),
                "efficiency": deque(maxlen=window),
                "flow_perm":  deque(maxlen=window),
            }
        )

    def add_metrics(self, device_id: str, metrics: Dict):
        buf = self._buffers[device_id]
        if metrics.get("delta_pressure_bar") is not None:
            buf["pressure"].append(metrics["delta_pressure_bar"])
        if metrics.get("efficiency") is not None:
            buf["efficiency"].append(metrics["efficiency"])
        if metrics.get("flow_perm_lpm") is not None:
            buf["flow_perm"].append(metrics["flow_perm_lpm"])

    def slope(self, device_id: str, variable: str) -> Optional[float]:
        values = list(self._buffers[device_id][variable])
        n = len(values)
        if n < 5:
            return None
        x_mean = (n - 1) / 2
        y_mean = sum(values) / n
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
        den = sum((i - x_mean) ** 2 for i in range(n))
        return num / den if den > 0 else 0.0

    def get_trends(self, device_id: str) -> Dict:
        return {
            "pressure_slope":   self.slope(device_id, "pressure"),
            "efficiency_slope": self.slope(device_id, "efficiency"),
            "flow_slope":       self.slope(device_id, "flow_perm"),
        }


trend_analyzer = TrendAnalyzer(window=THRESHOLDS["trend_window"])

# ============================================================
# RISK ENGINE
# ============================================================

class RiskEngine:
    """
    Calcula el riesgo operativo como un score numérico [0-100]
    y lo traduce a: LOW | MEDIUM | HIGH | CRITICAL

    El riesgo combina:
      - Diagnóstico actual (peso por código)
      - Tendencias activas (peso por tipo)
      - Confianza del diagnóstico (amplifica o atenúa)
    """

    def compute(
        self,
        final_root:  Any,   # DiagnosticResult
        trend_diags: List[Dict],
        confidence:  float,
    ) -> Dict:
        score = 0.0

        # Peso base del diagnóstico principal
        diag_weight = RISK_WEIGHTS.get(final_root.code, 0)
        # La confianza amplifica: 100% confianza → peso completo
        score += diag_weight * confidence

        # Bonus por tendencias activas
        for trend in trend_diags:
            trend_weight = RISK_WEIGHTS.get(trend["code"], 0)
            score += trend_weight * 0.5  # las tendencias pesan la mitad

        score = min(100.0, score)

        if score >= 80:
            level = "CRITICAL"
        elif score >= 50:
            level = "HIGH"
        elif score >= 25:
            level = "MEDIUM"
        else:
            level = "LOW"

        return {"level": level, "score": round(score, 1)}


risk_engine = RiskEngine()

# ============================================================
# DEGRADATION TRACKER
# ============================================================

class DegradationTracker:
    """
    Calcula la tendencia de degradación comparando la eficiencia
    actual contra la eficiencia de hace N días.

    Se ejecuta con rate-limiting (máx cada biz_refresh_sec por dispositivo)
    para no hacer queries pesadas a DB cada segundo.

    Retorna:
      degradation_pct:   porcentaje de pérdida (negativo = degradó)
      degradation_days:  ventana de días analizada
      degradation_label: texto legible para el cliente
    """

    def __init__(self):
        self._last_computed: Dict[str, float] = {}
        self._cache:         Dict[str, Dict]  = {}

    def should_compute(self, device_id: str) -> bool:
        refresh = THRESHOLDS["biz_refresh_sec"]
        return (time.time() - self._last_computed.get(device_id, 0)) >= refresh

    def compute(self, device_id: str, current_efficiency: Optional[float]) -> Dict:
        """
        Compara eficiencia actual con:
          1. Baseline aprendido (preferido)
          2. Promedio de hace N días en DB (fallback)
        """
        empty = {"degradation_pct": None, "degradation_days": None, "degradation_label": None}

        if current_efficiency is None:
            return empty

        days   = THRESHOLDS["degradation_window_days"]
        min_pct = THRESHOLDS["degradation_min_pct"]

        # Intentar comparar contra baseline aprendido
        baseline_eff = BaselineCache.get_efficiency_baseline(device_id)

        if baseline_eff and baseline_eff > 0:
            degradation_pct = ((current_efficiency - baseline_eff) / baseline_eff) * 100
            reference       = "baseline"
            reference_eff   = baseline_eff
        else:
            # Fallback: promedio de eficiencia de hace N días
            rows = db.fetchall(
                """
                SELECT AVG(efficiency)
                FROM metrics
                WHERE device_id = %s
                  AND time BETWEEN
                      (NOW() - INTERVAL '%s days' - INTERVAL '1 day')
                  AND (NOW() - INTERVAL '%s days')
                  AND efficiency IS NOT NULL
                """,
                (device_id, days, days - 1)
            )
            if not rows or rows[0][0] is None:
                return empty

            reference_eff   = rows[0][0]
            degradation_pct = ((current_efficiency - reference_eff) / reference_eff) * 100
            reference       = f"hace {days} días"

        self._last_computed[device_id] = time.time()

        # Solo reportar si la degradación supera el mínimo relevante
        if abs(degradation_pct) < min_pct:
            result = {
                "degradation_pct":   round(degradation_pct, 1),
                "degradation_days":  days,
                "degradation_label": f"Rendimiento estable (ref: {reference})",
            }
        elif degradation_pct < 0:
            result = {
                "degradation_pct":   round(degradation_pct, 1),
                "degradation_days":  days,
                "degradation_label": (
                    f"Pérdida de {abs(degradation_pct):.1f}% de rendimiento "
                    f"vs {reference}"
                ),
            }
        else:
            result = {
                "degradation_pct":   round(degradation_pct, 1),
                "degradation_days":  days,
                "degradation_label": (
                    f"Mejora de {degradation_pct:.1f}% vs {reference}"
                ),
            }

        self._cache[device_id] = result
        return result

    def get_cached(self, device_id: str) -> Dict:
        """Retorna el último resultado calculado sin ir a DB."""
        return self._cache.get(device_id, {
            "degradation_pct":   None,
            "degradation_days":  None,
            "degradation_label": None,
        })


degradation_tracker = DegradationTracker()

# ============================================================
# BUSINESS METRICS ENGINE
# ============================================================

class BusinessMetricsEngine:
    """
    Calcula los KPIs que responden las 3 preguntas del cliente:

      1. ¿Estoy produciendo lo que debería?
         → liters_today, target_liters, fulfillment_pct

      2. ¿Estoy desperdiciando agua/energía?
         → waste_liters_today, waste_pct

      3. ¿Mi equipo se está degradando?
         → degradation_pct, degradation_label

    Se ejecuta con rate limiting (biz_refresh_sec) para no
    sobrecargar la DB con queries cada segundo.

    Los resultados se guardan en device_status (tiempo real)
    y en business_metrics (histórico diario).
    """

    def __init__(self):
        self._last_run: Dict[str, float] = {}

    def should_run(self, device_id: str) -> bool:
        refresh = THRESHOLDS["biz_refresh_sec"]
        return (time.time() - self._last_run.get(device_id, 0)) >= refresh

    def compute(
        self,
        device_id:   str,
        timestamp:   datetime,
        metrics:     Optional[Dict],
        final_root:  Any,   # DiagnosticResult
        trend_diags: List[Dict],
    ) -> Dict:
        """Calcula y persiste todas las métricas de negocio."""

        config     = KPIEngine._get_config(device_id)
        tz_name    = 'America/Argentina/Buenos_Aires'

        # ── 1. PRODUCCIÓN DEL DÍA ────────────────────────────
        rows = db.fetchall(
            f"""
            SELECT
                MAX(volume_perm_l)    - MIN(volume_perm_l)    AS liters_produced,
                MAX(volume_rechazo_l) - MIN(volume_rechazo_l) AS liters_rejected
            FROM telemetry_process
            WHERE device_id = %s
              AND DATE(time AT TIME ZONE '{tz_name}') = CURRENT_DATE AT TIME ZONE '{tz_name}'
              AND volume_perm_l IS NOT NULL
            """,
            (device_id,)
        )

        liters_today       = rows[0][0] if rows and rows[0][0] else 0.0
        waste_liters_today = rows[0][1] if rows and rows[0][1] else 0.0
        total_today        = (liters_today or 0) + (waste_liters_today or 0)

        # Cumplimiento vs objetivo
        target         = config.get("daily_target_liters", 0) or 0
        fulfillment_pct = None
        if target > 0:
            fulfillment_pct = round(min(100.0, (liters_today / target) * 100), 1)

        # % de agua desperdiciada
        waste_pct = None
        if total_today > 0:
            waste_pct = round((waste_liters_today / total_today) * 100, 1)

        # ── 2. RIESGO ─────────────────────────────────────────
        risk = risk_engine.compute(final_root, trend_diags, final_root.confidence)

        # ── 3. DEGRADACIÓN ────────────────────────────────────
        current_eff = metrics.get("efficiency") if metrics else None
        if degradation_tracker.should_compute(device_id):
            deg = degradation_tracker.compute(device_id, current_eff)
        else:
            deg = degradation_tracker.get_cached(device_id)

        # ── 4. FRESCURA DEL DIAGNÓSTICO ──────────────────────
        rows_h = db.fetchall(
            "SELECT health_updated_at FROM device_status WHERE device_id = %s",
            (device_id,)
        )
        health_age_hours = None
        if rows_h and rows_h[0][0]:
            delta = datetime.now(tz=timezone.utc) - rows_h[0][0]
            health_age_hours = round(delta.total_seconds() / 3600, 1)

        self._last_run[device_id] = time.time()

        result = {
            "liters_today":       round(liters_today, 1),
            "target_liters":      target,
            "fulfillment_pct":    fulfillment_pct,
            "waste_liters_today": round(waste_liters_today, 1),
            "waste_pct":          waste_pct,
            "risk_level":         risk["level"],
            "risk_score":         risk["score"],
            "degradation_pct":    deg.get("degradation_pct"),
            "degradation_days":   deg.get("degradation_days"),
            "degradation_label":  deg.get("degradation_label"),
            "health_age_hours":   health_age_hours,
        }

        # Guardar en business_metrics (historial diario, UPSERT)
        self._persist_daily(device_id, timestamp, result, metrics)

        return result

    def _persist_daily(
        self,
        device_id: str,
        timestamp: datetime,
        biz:       Dict,
        metrics:   Optional[Dict],
    ):
        """Persiste o actualiza el registro del día en business_metrics."""
        avg_eff = avg_rec = None
        if metrics:
            avg_eff = metrics.get("efficiency")
            avg_rec = metrics.get("recovery")

        db.execute(
            """
            INSERT INTO business_metrics
              (day, device_id, liters_produced, liters_rejected,
               daily_target_liters, fulfillment_pct,
               waste_pct, avg_efficiency, avg_recovery,
               risk_level, calculated_at)
            VALUES (
              CURRENT_DATE AT TIME ZONE 'America/Argentina/Buenos_Aires',
              %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (day, device_id) DO UPDATE SET
              liters_produced      = EXCLUDED.liters_produced,
              liters_rejected      = EXCLUDED.liters_rejected,
              daily_target_liters  = EXCLUDED.daily_target_liters,
              fulfillment_pct      = EXCLUDED.fulfillment_pct,
              waste_pct            = EXCLUDED.waste_pct,
              avg_efficiency       = EXCLUDED.avg_efficiency,
              avg_recovery         = EXCLUDED.avg_recovery,
              risk_level           = EXCLUDED.risk_level,
              calculated_at        = NOW()
            """,
            (
                device_id,
                biz["liters_today"],       biz["waste_liters_today"],
                biz["target_liters"],      biz["fulfillment_pct"],
                biz["waste_pct"],          avg_eff,
                avg_rec,                   biz["risk_level"],
            )
        )


biz_engine = BusinessMetricsEngine()

# ============================================================
# DIAGNOSTIC RESULT
# ============================================================

class DiagnosticResult:
    def __init__(
        self,
        severity:   str,
        code:       str,
        message:    str,
        action:     str,
        evidence:   Dict,
        symptoms:   Dict  = None,
        score:      int   = 0,
        confidence: float = 1.0,
        is_event:   bool  = False,
    ):
        self.severity   = severity
        self.code       = code
        self.message    = message
        self.action     = action
        self.evidence   = evidence
        self.symptoms   = symptoms or {}
        self.score      = score
        self.confidence = confidence
        self.is_event   = is_event

    def __repr__(self):
        return f"[{self.severity}|score={self.score}|conf={self.confidence:.0%}] {self.code}"

    def to_dict(self):
        return {
            "severity":   self.severity,  "code":       self.code,
            "message":    self.message,   "action":     self.action,
            "evidence":   self.evidence,  "symptoms":   self.symptoms,
            "score":      self.score,     "confidence": round(self.confidence, 3),
            "is_event":   self.is_event,
        }


DIAG_OK = DiagnosticResult(
    severity="OK", code="NORMAL",
    message="Operando normalmente",
    action="Sin acción requerida.",
    evidence={}, score=0, confidence=1.0,
)

# ============================================================
# NO-FLOW TRACKER
# ============================================================

class NoFlowTracker:
    def __init__(self):
        self._no_flow_since: Dict[str, Optional[float]] = {}

    def update(self, device_id: str, state: str, flow_perm: Optional[float]) -> bool:
        if state not in ACTIVE_STATES:
            self._no_flow_since[device_id] = None
            return False

        has_flow = flow_perm is not None and flow_perm > 0.05
        if has_flow:
            self._no_flow_since[device_id] = None
            return False

        now = time.time()
        if self._no_flow_since.get(device_id) is None:
            self._no_flow_since[device_id] = now
            return False

        return (now - self._no_flow_since[device_id]) >= THRESHOLDS["no_flow_timeout_sec"]

    def get_duration(self, device_id: str) -> float:
        since = self._no_flow_since.get(device_id)
        return (time.time() - since) if since else 0.0


no_flow_tracker = NoFlowTracker()

# ============================================================
# HYSTERESIS MANAGER
# ============================================================

class HysteresisManager:
    def __init__(self):
        self._state: Dict[str, Dict[str, Dict]] = defaultdict(dict)

    def update(
        self,
        device_id:    str,
        active_codes: List[str],
        confirm_sec:  int = 60,
        clear_sec:    int = 120,
    ) -> List[str]:
        now   = time.time()
        state = self._state[device_id]

        for code in active_codes:
            if code not in state:
                state[code] = {
                    "first_seen": now, "last_seen": now,
                    "confirmed": False, "cleared_since": None,
                }
            else:
                state[code]["last_seen"]     = now
                state[code]["cleared_since"] = None

            if not state[code]["confirmed"]:
                if now - state[code]["first_seen"] >= confirm_sec:
                    state[code]["confirmed"] = True
                    log.info(f"[{device_id}] Diagnóstico confirmado: {code}")

        to_delete = []
        for code, info in state.items():
            if code not in active_codes:
                if info["cleared_since"] is None:
                    info["cleared_since"] = now
                elif now - info["cleared_since"] >= clear_sec:
                    to_delete.append(code)
                    log.info(f"[{device_id}] Diagnóstico limpiado: {code}")
        for code in to_delete:
            del state[code]

        return [code for code, info in state.items() if info["confirmed"]]

    def is_new_confirmation(self, device_id: str, code: str) -> bool:
        info = self._state[device_id].get(code)
        if not info or not info["confirmed"]:
            return False
        return (time.time() - info["first_seen"]) < THRESHOLDS["hysteresis_confirm_sec"] + 5


hysteresis = HysteresisManager()

# ============================================================
# DIAGNOSTIC ENGINE
# ============================================================

class DiagnosticEngine:

    def run(self, device_id, process, metrics, state, inputs, trends) -> Dict:
        thresh    = BaselineCache.get(device_id)
        all_diags: List[DiagnosticResult] = []

        all_diags.extend(self._eval_events(state, inputs, process, device_id))
        if metrics:
            all_diags.extend(self._eval_operational(metrics, process, thresh))

        trend_diags = []
        if metrics and trends:
            trend_diags = self._eval_trends(metrics, trends)

        if not all_diags:
            return {"root_cause": DIAG_OK, "all_diags": [], "trend_diags": trend_diags}

        all_diags.sort(key=lambda d: d.score, reverse=True)
        root = all_diags[0]
        root.confidence = self._calc_confidence(root, all_diags, metrics or {})
        for other in all_diags[1:]:
            root.symptoms.update(other.evidence)

        return {"root_cause": root, "all_diags": all_diags, "trend_diags": trend_diags}

    def _calc_confidence(self, diag, all_diags, metrics) -> float:
        base            = 0.6
        evidence_bonus  = min(0.3, len(diag.evidence) * 0.1)
        coherence_bonus = 0.1 if len(all_diags) == 1 else (
            0.1 if (diag.score - all_diags[1].score) < 20 else 0.0
        )
        return min(1.0, base + evidence_bonus + coherence_bonus)

    def _eval_events(self, state, inputs, process, device_id) -> List[DiagnosticResult]:
        results = []

        if state == "FAULT":
            crudo = (inputs or {}).get("crudo_ok", True)
            retry = process.get("retry_count", 0)
            if not crudo:
                results.append(DiagnosticResult(
                    "CRITICAL", "FAULT_NO_WATER",
                    "FALLA: Sin agua de crudo. El sistema no puede arrancar.",
                    "Verificar suministro de agua cruda, flotante del tanque y válvula de entrada.",
                    {"state": state, "crudo_ok": False, "retry": retry},
                    score=DIAG_SCORES["FAULT_NO_WATER"], is_event=True,
                ))
            else:
                results.append(DiagnosticResult(
                    "CRITICAL", "FAULT_SYSTEM",
                    f"FALLA DEL SISTEMA: retries agotados ({retry}).",
                    "Resetear equipo. Si persiste, inspeccionar bomba y presostato.",
                    {"state": state, "retry": retry},
                    score=DIAG_SCORES["FAULT_SYSTEM"], is_event=True,
                ))

        elif inputs and not inputs.get("crudo_ok", True) and state not in ("IDLE", "FLUSHING"):
            results.append(DiagnosticResult(
                "CRITICAL", "NO_RAW_WATER",
                "Sin agua de crudo mientras el equipo intenta operar.",
                "Verificar tanque de agua cruda y señal del flotante.",
                {"crudo_ok": False, "state": state},
                score=DIAG_SCORES["NO_RAW_WATER"], is_event=True,
            ))

        flow_p = validate_float(process.get("flow_perm_lpm"), 0, 100)
        if no_flow_tracker.update(device_id, state, flow_p):
            duration = no_flow_tracker.get_duration(device_id)
            p_mem    = validate_float(process.get("pressure_membrane_bar"), 0, 50)
            results.append(DiagnosticResult(
                "CRITICAL", "NO_FLOW_DETECTED",
                f"Equipo en producción sin caudal de permeado por {duration:.0f}s.",
                "Verificar bomba de alta presión, válvula de permeado y membrana. "
                "Revisar si hay aire atrapado en el sistema.",
                {
                    "flow_perm_lpm":    flow_p or 0,
                    "state":            state,
                    "duration_sec":     round(duration, 0),
                    "pressure_membrane": p_mem,
                    "confidence_note":  "Verificar sensor de caudal antes de actuar",
                },
                score=DIAG_SCORES["NO_FLOW_DETECTED"], is_event=True,
            ))

        return results

    def _eval_operational(self, metrics, process, thresh) -> List[DiagnosticResult]:
        results = []
        p_mem   = validate_float(process.get("pressure_membrane_bar"), 0, 50)
        flow_p  = metrics.get("flow_perm_lpm")
        eff     = metrics.get("efficiency")
        rec     = metrics.get("recovery")
        delta_p = metrics.get("delta_pressure_bar")

        if p_mem is not None and p_mem > thresh["pressure_crit_high"]:
            results.append(DiagnosticResult(
                "CRITICAL", "HIGH_PRESSURE",
                f"Presión crítica: {p_mem:.1f} bar (límite {thresh['pressure_crit_high']:.1f} bar).",
                "Detener equipo. Verificar válvula de rechazo y estado de membrana.",
                {"pressure_membrane_bar": p_mem},
                score=DIAG_SCORES["HIGH_PRESSURE"], is_event=True,
            ))

        if eff is not None and eff < thresh["efficiency_crit_low"]:
            results.append(DiagnosticResult(
                "CRITICAL", "CRITICAL_EFFICIENCY",
                f"Eficiencia crítica: {eff*100:.1f}% (límite {thresh['efficiency_crit_low']*100:.0f}%).",
                "Inspeccionar membrana. Posible rotura o fouling severo.",
                {"efficiency": eff},
                score=DIAG_SCORES["CRITICAL_EFFICIENCY"], is_event=True,
            ))

        if (p_mem is not None and p_mem > thresh["pressure_warn_high"] * 0.85 and
                flow_p is not None and flow_p < thresh["flow_perm_warn_low"]):
            extra = {"delta_pressure_bar": delta_p} if delta_p and delta_p > 1.5 else {}
            results.append(DiagnosticResult(
                "WARNING", "MEMBRANE_FOULING",
                f"Ensuciamiento de membrana: presión alta ({p_mem:.1f} bar) con bajo caudal ({flow_p:.2f} L/min).",
                "Ejecutar ciclo de limpieza CIP. Si persiste, reemplazar membrana.",
                {"pressure_membrane_bar": p_mem, "flow_perm_lpm": flow_p, **extra},
                score=DIAG_SCORES["MEMBRANE_FOULING"],
            ))

        if (eff is not None and eff < thresh["efficiency_warn_low"] and
                rec is not None and rec > thresh["recovery_warn_high"]):
            results.append(DiagnosticResult(
                "WARNING", "MEMBRANE_SCALING",
                f"Incrustación (scaling): eficiencia {eff*100:.1f}% con recovery alto {rec*100:.1f}%.",
                "Revisar dosis de antiscalante. Reducir recovery. Programar limpieza ácida.",
                {"efficiency": eff, "recovery": rec},
                score=DIAG_SCORES["MEMBRANE_SCALING"],
            ))

        if (eff is not None and eff < thresh["efficiency_warn_low"] and
                p_mem is not None and
                THRESHOLDS["pressure_low_bar"] < p_mem < thresh["pressure_warn_high"] * 0.8):
            results.append(DiagnosticResult(
                "WARNING", "MEMBRANE_DEGRADED",
                f"Degradación de membrana: eficiencia {eff*100:.1f}% con presión normal ({p_mem:.1f} bar).",
                "Evaluar reemplazo de membrana. Verificar cloro residual en agua de entrada.",
                {"efficiency": eff, "pressure_membrane_bar": p_mem},
                score=DIAG_SCORES["MEMBRANE_DEGRADED"],
            ))

        if rec is not None and rec < thresh["recovery_warn_low"]:
            results.append(DiagnosticResult(
                "WARNING", "LOW_RECOVERY",
                f"Recovery bajo: {rec*100:.1f}%.",
                "Ajustar válvula de rechazo. Verificar configuración de caudales.",
                {"recovery": rec},
                score=DIAG_SCORES["LOW_RECOVERY"],
            ))

        if eff is not None and eff < thresh["efficiency_warn_low"]:
            results.append(DiagnosticResult(
                "WARNING", "LOW_EFFICIENCY",
                f"Eficiencia baja: {eff*100:.1f}%.",
                "Revisión general: calidad de agua cruda, estado de membrana y pretratamiento.",
                {"efficiency": eff},
                score=DIAG_SCORES["LOW_EFFICIENCY"],
            ))

        if flow_p is not None and flow_p < thresh["flow_perm_warn_low"]:
            results.append(DiagnosticResult(
                "WARNING", "LOW_PERMEATE_FLOW",
                f"Caudal de permeado bajo: {flow_p:.2f} L/min.",
                "Verificar presión de entrada y estado de membrana.",
                {"flow_perm_lpm": flow_p},
                score=DIAG_SCORES["LOW_PERMEATE_FLOW"],
            ))

        if p_mem is not None and p_mem < THRESHOLDS["pressure_low_bar"]:
            results.append(DiagnosticResult(
                "WARNING", "LOW_PRESSURE",
                f"Presión de membrana baja: {p_mem:.1f} bar.",
                "Verificar bomba de alta presión y válvulas.",
                {"pressure_membrane_bar": p_mem},
                score=DIAG_SCORES["LOW_PRESSURE"],
            ))

        return results

    def _eval_trends(self, metrics, trends) -> List[Dict]:
        trend_list = []
        p_slope   = trends.get("pressure_slope")
        eff_slope = trends.get("efficiency_slope")
        delta_p   = metrics.get("delta_pressure_bar")
        eff       = metrics.get("efficiency")

        if (p_slope is not None and
                p_slope > THRESHOLDS["pressure_trend_threshold"] and
                delta_p is not None and delta_p > 0.5):
            trend_list.append({
                "code":      "FOULING_PROGRESSIVE",
                "message":   f"Presión diferencial subiendo (+{p_slope:.4f} bar/muestra).",
                "direction": "up",
                "variable":  "delta_pressure_bar",
                "slope":     p_slope,
            })

        if (eff_slope is not None and
                eff_slope < THRESHOLDS["efficiency_trend_threshold"] and
                eff is not None):
            trend_list.append({
                "code":      "DECLINING_EFFICIENCY",
                "message":   f"Eficiencia en tendencia descendente ({eff_slope*100:.3f}%/muestra).",
                "direction": "down",
                "variable":  "efficiency",
                "slope":     eff_slope,
            })

        return trend_list


diagnostic_engine = DiagnosticEngine()

# ============================================================
# STATE TRACKER
# ============================================================

class DeviceStateTracker:
    def __init__(self):
        self._state:   Dict[str, str]  = defaultdict(lambda: "UNKNOWN")
        self._inputs:  Dict[str, Dict] = {}
        self._process: Dict[str, Dict] = {}

    def update_state(self, device_id, state):   self._state[device_id]   = state
    def update_inputs(self, device_id, data):   self._inputs[device_id]  = data
    def update_process(self, device_id, data):  self._process[device_id] = data
    def get_state(self, device_id):   return self._state.get(device_id, "UNKNOWN")
    def get_inputs(self, device_id):  return self._inputs.get(device_id)
    def get_process(self, device_id): return self._process.get(device_id)


tracker = DeviceStateTracker()

# ============================================================
# ALERT MANAGER
# ============================================================

class AlertManager:
    def __init__(self):
        self._last_alert:    Dict[str, float] = {}
        self._last_code:     Dict[str, str]   = {}
        self._last_severity: Dict[str, str]   = {}
        self._chat_cache:    Dict[str, Optional[str]] = {}

    def _get_chat(self, device_id: str) -> Optional[str]:
        if device_id in self._chat_cache:
            return self._chat_cache[device_id]
        rows = db.fetchall(
            "SELECT telegram_chat_id FROM devices WHERE device_id = %s", (device_id,)
        )
        chat_id = rows[0][0] if rows and rows[0][0] else TELEGRAM_ADMIN_CHAT
        self._chat_cache[device_id] = chat_id
        return chat_id

    def invalidate_cache(self, device_id: str):
        self._chat_cache.pop(device_id, None)

    def process(self, device_id: str, root: DiagnosticResult, is_new: bool, biz: Dict):
        if root.severity == "OK":
            self._last_code[device_id]     = "NORMAL"
            self._last_severity[device_id] = "OK"
            return

        now           = time.time()
        cooldown      = THRESHOLDS["alert_cooldown_sec"]
        prev_code     = self._last_code.get(device_id, "NORMAL")
        prev_severity = self._last_severity.get(device_id, "OK")

        should_alert = False
        reason       = ""

        if root.is_event and (now - self._last_alert.get(device_id, 0)) >= cooldown:
            should_alert = True
            reason       = "evento crítico"
        elif is_new and root.code != prev_code:
            should_alert = True
            reason       = "nuevo diagnóstico confirmado"
        elif (root.severity == "CRITICAL" and prev_severity in ("OK", "WARNING") and
              (now - self._last_alert.get(device_id, 0)) >= cooldown):
            should_alert = True
            reason       = "escalada a CRITICAL"

        if not should_alert:
            return

        chat_id = self._get_chat(device_id)
        if not chat_id or not TELEGRAM_TOKEN:
            log.warning(f"Sin chat_id para {device_id}")
            return

        icon     = "🚨" if root.severity == "CRITICAL" else "⚠️"
        conf_pct = int(root.confidence * 100)
        ev_lines = "\n".join(f"  ✔ {k}: {v}" for k, v in root.evidence.items())
        sy_lines = ("\n".join(f"  • {k}: {v}" for k, v in root.symptoms.items())
                    if root.symptoms else "  —")

        # Contexto de negocio en la alerta
        biz_lines = ""
        if biz.get("degradation_label"):
            biz_lines += f"\n📉 {biz['degradation_label']}"
        if biz.get("waste_pct") is not None:
            biz_lines += f"\n💧 Desperdicio hoy: {biz['waste_pct']:.1f}%"
        if biz.get("fulfillment_pct") is not None:
            biz_lines += f"\n🎯 Cumplimiento: {biz['fulfillment_pct']:.1f}% del objetivo"

        msg = (
            f"{icon} *FYNTEK [{device_id}]*\n"
            f"*{root.code}* — Riesgo: {biz.get('risk_level', '?')} "
            f"(confianza: {conf_pct}%)\n\n"
            f"📋 {root.message}\n\n"
            f"🔧 *Acción:* {root.action}\n\n"
            f"*Evidencias:*\n{ev_lines}\n\n"
            f"*Síntomas:*\n{sy_lines}"
            f"{biz_lines}"
        )

        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
                timeout=5,
            )
            if resp.ok:
                self._last_alert[device_id]    = now
                self._last_code[device_id]     = root.code
                self._last_severity[device_id] = root.severity
                log.info(f"📱 Telegram [{device_id}] {root.code} ({reason})")
            else:
                log.error(f"Telegram error {resp.status_code}")
        except Exception as e:
            log.error(f"Telegram excepción: {e}")


alert_manager = AlertManager()

# ============================================================
# LEARN ENGINE
# ============================================================

class LearnEngine:
    def __init__(self):
        self._active: Dict[str, Dict] = {}

    def start(self, device_id: str, duration_minutes: int = 30) -> Optional[int]:
        if device_id in self._active:
            self.cancel(device_id)
        session_id = db.insert_returning(
            "INSERT INTO learning_sessions (device_id, started_at, duration_min) "
            "VALUES (%s, NOW(), %s) RETURNING id",
            (device_id, duration_minutes)
        )
        if not session_id:
            return None
        self._active[device_id] = {
            "session_id":   session_id,
            "started_at":   time.time(),
            "duration_sec": duration_minutes * 60,
            "samples":      [],
        }
        log.info(f"[{device_id}] Learn iniciado — {duration_minutes} min")
        return session_id

    def is_active(self, device_id: str) -> bool:
        return device_id in self._active

    def cancel(self, device_id: str):
        session = self._active.pop(device_id, None)
        if session:
            db.execute(
                "UPDATE learning_sessions SET status='CANCELLED', finished_at=NOW() WHERE id=%s",
                (session["session_id"],)
            )

    def add_sample(self, device_id: str, metrics: Dict):
        if device_id not in self._active:
            return
        session = self._active[device_id]
        session["samples"].append({k: v for k, v in metrics.items() if v is not None})
        if len(session["samples"]) % 50 == 0:
            db.execute(
                "UPDATE learning_sessions SET samples=%s WHERE id=%s",
                (len(session["samples"]), session["session_id"])
            )
        if time.time() - session["started_at"] >= session["duration_sec"]:
            self._finish(device_id)

    def _finish(self, device_id: str):
        session = self._active.pop(device_id)
        samples = session["samples"]
        n       = len(samples)

        if n < 10:
            log.warning(f"[{device_id}] Learn cancelado: pocas muestras ({n})")
            db.execute(
                "UPDATE learning_sessions SET status='CANCELLED', finished_at=NOW(), samples=%s WHERE id=%s",
                (n, session["session_id"])
            )
            return

        def stats(key):
            vals = [s[key] for s in samples if key in s and s[key] is not None]
            if len(vals) < 5:
                return None, None
            mean = sum(vals) / len(vals)
            std  = (sum((v - mean) ** 2 for v in vals) / len(vals)) ** 0.5
            return mean, std

        eff_m, eff_s = stats("efficiency")
        rec_m, rec_s = stats("recovery")
        fp_m,  fp_s  = stats("flow_perm_lpm")
        dp_m,  dp_s  = stats("delta_pressure_bar")

        def w_high(m, s, mult=2): return (m + mult * s) if m is not None and s else None
        def w_low(m, s, mult=2):  return max(0, m - mult * s) if m is not None and s else None

        db.execute(
            "INSERT INTO device_baseline (device_id, learned_at, session_id) VALUES (%s, NOW(), %s) "
            "ON CONFLICT (device_id) DO NOTHING",
            (device_id, session["session_id"])
        )
        db.execute(
            """
            UPDATE device_baseline SET
                learned_at = NOW(), session_id = %s,
                efficiency_mean = %s, efficiency_std = %s,
                recovery_mean = %s, recovery_std = %s,
                flow_perm_mean = %s, flow_perm_std = %s,
                delta_pressure_mean = %s, delta_pressure_std = %s,
                efficiency_warn_low_learned = %s,
                efficiency_crit_low_learned = %s,
                recovery_warn_low_learned = %s,
                recovery_warn_high_learned = %s,
                flow_perm_warn_low_learned = %s,
                pressure_warn_high_learned = %s,
                pressure_crit_high_learned = %s,
                delta_pressure_warn_high_learned = %s
            WHERE device_id = %s
            """,
            (
                session["session_id"],
                eff_m, eff_s, rec_m, rec_s, fp_m, fp_s, dp_m, dp_s,
                w_low(eff_m, eff_s, 2),  w_low(eff_m, eff_s, 3),
                w_low(rec_m, rec_s, 2),  w_high(rec_m, rec_s, 2),
                w_low(fp_m, fp_s, 2),
                w_high(dp_m, dp_s, 2),   w_high(dp_m, dp_s, 3),
                w_high(dp_m, dp_s, 2),
                device_id,
            )
        )
        db.execute(
            "UPDATE learning_sessions SET status='DONE', finished_at=NOW(), samples=%s WHERE id=%s",
            (n, session["session_id"])
        )
        BaselineCache.invalidate(device_id)
        log.info(f"[{device_id}] Baseline learned actualizado ({n} muestras)")


learn_engine = LearnEngine()

# ============================================================
# MESSAGE PROCESSOR
# ============================================================

class MessageProcessor:

    def dispatch(self, topic: str, payload: str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            log.warning(f"JSON inválido en {topic}: {e}")
            return

        device_id = data.get("device_id", "unknown")
        timestamp = ts_to_utc(data.get("ts"))
        if timestamp is None:
            log.warning(f"[{device_id}] timestamp inválido, descartando")
            return

        parts = topic.split("/")
        if len(parts) < 3:
            return

        handlers = {
            "process":   self._handle_process,
            "quality":   self._handle_quality,
            "state":     self._handle_state,
            "inputs":    self._handle_inputs,
            "outputs":   self._handle_outputs,
            "heartbeat": self._handle_heartbeat,
        }
        handler = handlers.get(parts[2])
        if handler:
            handler(device_id, timestamp, data)

    def _auto_register(self, device_id: str, fw_version: str = ""):
        db.execute(
            "INSERT INTO devices (device_id, fw_version, registered_at) VALUES (%s,%s,NOW()) "
            "ON CONFLICT (device_id) DO UPDATE SET fw_version = EXCLUDED.fw_version",
            (device_id, fw_version)
        )

    def _handle_process(self, device_id, timestamp, data):
        db.execute(
            "INSERT INTO telemetry_process "
            "(time,device_id,flow_perm_lpm,flow_rechazo_lpm,pressure_membrane_bar,"
            "pressure_brine_bar,volume_perm_l,volume_rechazo_l,fw_version) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                timestamp, device_id,
                validate_float(data.get("flow_perm_lpm"),          0, 100),
                validate_float(data.get("flow_rechazo_lpm"),        0, 100),
                validate_float(data.get("pressure_membrane_bar"),   0, 50),
                validate_float(data.get("pressure_brine_bar"),      0, 50),
                validate_float(data.get("volume_perm_l"),           0, 1e7),
                validate_float(data.get("volume_rechazo_l"),        0, 1e7),
                data.get("fw_version", ""),
            ),
        )
        tracker.update_process(device_id, data)
        self._run_analytics(device_id, timestamp, data)

    def _handle_quality(self, device_id, timestamp, data):
        tds_in  = validate_float(data.get("tds_in_ppm"),  0, 5)
        tds_out = validate_float(data.get("tds_out_ppm"), 0, 5)
        db.execute(
            "INSERT INTO telemetry_quality (time,device_id,tds_in_raw,tds_out_raw,fw_version) "
            "VALUES (%s,%s,%s,%s,%s)",
            (timestamp, device_id, tds_in, tds_out, data.get("fw_version", "")),
        )
        KPIEngine.update_quality_cache(device_id, {"tds_in_raw": tds_in, "tds_out_raw": tds_out})

    def _handle_state(self, device_id, timestamp, data):
        state = data.get("state", "UNKNOWN")
        db.execute(
            "INSERT INTO telemetry_state (time,device_id,state,state_numeric,running,retry_count) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (timestamp, device_id, state, STATE_MAP.get(state, -1),
             validate_bool(data.get("running")), data.get("retry", 0)),
        )
        tracker.update_state(device_id, state)
        log.info(f"[{device_id}] Estado → {state}")
        if state == "FAULT":
            self._run_analytics(device_id, timestamp, tracker.get_process(device_id) or {})

    def _handle_inputs(self, device_id, timestamp, data):
        inputs = {k: validate_bool(data.get(k))
                  for k in ("demand","crudo_ok","dose_ok","presostato","reserva1","reserva2")}
        db.execute(
            "INSERT INTO telemetry_inputs "
            "(time,device_id,demand,crudo_ok,dose_ok,presostato,reserva1,reserva2) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (timestamp, device_id, *inputs.values()),
        )
        tracker.update_inputs(device_id, inputs)

    def _handle_outputs(self, device_id, timestamp, data):
        db.execute(
            "INSERT INTO telemetry_outputs "
            "(time,device_id,pump_low,pump_high,pump_inlet,pump_dose,valve_flush,valve_inlet) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (timestamp, device_id,
             validate_bool(data.get("pump_low")),    validate_bool(data.get("pump_high")),
             validate_bool(data.get("pump_inlet")),  validate_bool(data.get("pump_dose")),
             validate_bool(data.get("valve_flush")), validate_bool(data.get("valve_inlet"))),
        )

    def _handle_heartbeat(self, device_id, timestamp, data):
        self._auto_register(device_id, data.get("fw_version", ""))
        db.execute(
            "INSERT INTO device_status (device_id,last_seen,online) VALUES (%s,%s,TRUE) "
            "ON CONFLICT (device_id) DO UPDATE "
            "SET last_seen=EXCLUDED.last_seen, online=TRUE",
            (device_id, timestamp),
        )

    # ---- ANALYTICS PIPELINE ────────────────────────────────

    def _run_analytics(self, device_id: str, timestamp: datetime, process_data: Dict):
        state  = tracker.get_state(device_id)
        inputs = tracker.get_inputs(device_id)
        physical = KPIEngine.read_physical(process_data)
        metrics  = KPIEngine.compute_kpis(device_id, physical, state)

        if metrics:
            trend_analyzer.add_metrics(device_id, metrics)
            if learn_engine.is_active(device_id):
                learn_engine.add_sample(device_id, metrics)
            db.execute(
                "INSERT INTO metrics "
                "(time,device_id,recovery,efficiency,rejection_ratio,delta_pressure_bar,"
                "flow_perm_lpm,flow_rechazo_lpm,tds_in_raw,tds_out_raw,cost_per_liter) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (timestamp, device_id,
                 metrics["recovery"],        metrics["efficiency"],
                 metrics["rejection_ratio"], metrics["delta_pressure_bar"],
                 metrics["flow_perm_lpm"],   metrics["flow_rechazo_lpm"],
                 metrics["tds_in_raw"],      metrics["tds_out_raw"],
                 metrics["cost_per_liter"]),
            )

        trends = trend_analyzer.get_trends(device_id) if metrics else None
        result = diagnostic_engine.run(device_id, process_data, metrics, state, inputs, trends)

        root        = result["root_cause"]
        all_diags   = result["all_diags"]
        trend_diags = result["trend_diags"]

        # Histéresis
        event_diags    = [d for d in all_diags if d.is_event]
        slow_diags     = [d for d in all_diags if not d.is_event]
        confirmed_slow = hysteresis.update(
            device_id, [d.code for d in slow_diags],
            confirm_sec=THRESHOLDS["hysteresis_confirm_sec"],
            clear_sec=THRESHOLDS["hysteresis_clear_sec"],
        )

        final_root = DIAG_OK
        is_new     = False

        if event_diags:
            final_root = event_diags[0]
            is_new     = True
        elif confirmed_slow:
            confirmed_diags = sorted(
                [d for d in slow_diags if d.code in confirmed_slow],
                key=lambda d: d.score, reverse=True
            )
            final_root = confirmed_diags[0]
            final_root.confidence = diagnostic_engine._calc_confidence(
                final_root, confirmed_diags, metrics or {}
            )
            for other in confirmed_diags[1:]:
                final_root.symptoms.update(other.evidence)
            is_new = hysteresis.is_new_confirmation(device_id, final_root.code)

        # Persistir diagnóstico
        if final_root.severity != "OK":
            db.execute(
                "INSERT INTO diagnostics "
                "(time,device_id,severity,code,message,action,details) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (timestamp, device_id, final_root.severity, final_root.code,
                 final_root.message, final_root.action, json.dumps(final_root.to_dict())),
            )
            log.info(f"[{device_id}] {final_root}")

        # ── Métricas de negocio (con rate limiting) ──────────
        biz: Dict = {}
        if biz_engine.should_run(device_id):
            biz = biz_engine.compute(device_id, timestamp, metrics, final_root, trend_diags)

        # ── Actualizar device_status ──────────────────────────
        new_health        = map_severity_to_health(final_root.severity)
        update_health     = state in ACTIVE_STATES or final_root.severity != "OK"

        if update_health:
            db.execute(
                """
                INSERT INTO device_status
                (device_id, last_seen, online, state,
                last_severity, last_diag_code, last_diag_message, last_action,
                flow_perm_lpm, pressure_membrane, recovery, efficiency,
                health_status, health_code, health_message,
                health_action, health_updated_at,
                biz_liters_today, biz_target_liters, biz_fulfillment_pct,
                biz_waste_liters_today, biz_waste_pct,
                biz_risk_level, biz_risk_score,
                biz_degradation_pct, biz_degradation_days, biz_degradation_label,
                biz_health_age_hours)
                VALUES (%s,%s,TRUE,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (device_id) DO UPDATE SET
                last_seen          = EXCLUDED.last_seen,
                online             = TRUE,
                state              = EXCLUDED.state,
                last_severity      = EXCLUDED.last_severity,
                last_diag_code     = EXCLUDED.last_diag_code,
                last_diag_message  = EXCLUDED.last_diag_message,
                last_action        = EXCLUDED.last_action,
                flow_perm_lpm      = EXCLUDED.flow_perm_lpm,
                pressure_membrane  = EXCLUDED.pressure_membrane,
                recovery           = EXCLUDED.recovery,
                efficiency         = EXCLUDED.efficiency,
                health_status      = EXCLUDED.health_status,
                health_code        = EXCLUDED.health_code,
                health_message     = EXCLUDED.health_message,
                health_action      = EXCLUDED.health_action,
                health_updated_at  = EXCLUDED.health_updated_at,
                biz_liters_today       = COALESCE(EXCLUDED.biz_liters_today, device_status.biz_liters_today),
                biz_target_liters      = COALESCE(EXCLUDED.biz_target_liters, device_status.biz_target_liters),
                biz_fulfillment_pct    = COALESCE(EXCLUDED.biz_fulfillment_pct, device_status.biz_fulfillment_pct),
                biz_waste_liters_today = COALESCE(EXCLUDED.biz_waste_liters_today, device_status.biz_waste_liters_today),
                biz_waste_pct          = COALESCE(EXCLUDED.biz_waste_pct, device_status.biz_waste_pct),
                biz_risk_level         = COALESCE(EXCLUDED.biz_risk_level, device_status.biz_risk_level),
                biz_risk_score         = COALESCE(EXCLUDED.biz_risk_score, device_status.biz_risk_score),
                biz_degradation_pct    = COALESCE(EXCLUDED.biz_degradation_pct, device_status.biz_degradation_pct),
                biz_degradation_days   = COALESCE(EXCLUDED.biz_degradation_days, device_status.biz_degradation_days),
                biz_degradation_label  = COALESCE(EXCLUDED.biz_degradation_label, device_status.biz_degradation_label),
                biz_health_age_hours   = EXCLUDED.biz_health_age_hours
                """,
                (
                    device_id,
                    timestamp,
                    state,
                    final_root.severity,
                    final_root.code,
                    final_root.message,
                    final_root.action,

                    # 🔹 SOLO variables físicas válidas
                    physical.get("flow_perm_lpm"),
                    physical.get("pressure_membrane_bar"),

                    metrics["recovery"]   if metrics else None,
                    metrics["efficiency"] if metrics else None,

                    new_health,
                    final_root.code,
                    final_root.message,
                    final_root.action,
                    timestamp,

                    biz.get("liters_today"),
                    biz.get("target_liters"),
                    biz.get("fulfillment_pct"),
                    biz.get("waste_liters_today"),
                    biz.get("waste_pct"),
                    biz.get("risk_level"),
                    biz.get("risk_score"),
                    biz.get("degradation_pct"),
                    biz.get("degradation_days"),
                    biz.get("degradation_label"),
                    biz.get("health_age_hours"),
                ),
            )      
        else:
            # Estado pasivo + OK: solo actualizamos campos operativos, no health
            db.execute(
                """
                INSERT INTO device_status
                (device_id, last_seen, online, state,
                last_severity, last_diag_code, last_diag_message, last_action,
                flow_perm_lpm, pressure_membrane, recovery, efficiency,
                biz_health_age_hours)
                VALUES (%s,%s,TRUE,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (device_id) DO UPDATE SET
                last_seen         = EXCLUDED.last_seen,
                online            = TRUE,
                state             = EXCLUDED.state,
                last_severity     = EXCLUDED.last_severity,
                last_diag_code    = EXCLUDED.last_diag_code,
                last_diag_message = EXCLUDED.last_diag_message,
                last_action       = EXCLUDED.last_action,
                flow_perm_lpm     = EXCLUDED.flow_perm_lpm,
                pressure_membrane = EXCLUDED.pressure_membrane,
                recovery          = EXCLUDED.recovery,
                efficiency        = EXCLUDED.efficiency,
                biz_health_age_hours = EXCLUDED.biz_health_age_hours
                """,
                (
                    device_id,
                    timestamp,
                    state,
                    final_root.severity,
                    final_root.code,
                    final_root.message,
                    final_root.action,

                    # 🔹 SOLO físicas
                    physical.get("flow_perm_lpm"),
                    physical.get("pressure_membrane_bar"),

                    metrics["recovery"]   if metrics else None,
                    metrics["efficiency"] if metrics else None,

                    biz.get("health_age_hours"),
                ),
            )

        alert_manager.process(device_id, final_root, is_new, biz)


processor = MessageProcessor()

# ============================================================
# MQTT
# ============================================================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(MQTT_TOPIC)
        log.info(f"✅ MQTT conectado → {MQTT_TOPIC}")
    else:
        log.error(f"❌ MQTT error (rc={rc})")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning(f"⚡ MQTT desconectado (rc={rc}). Reconectando...")

def on_message(client, userdata, msg):
    try:
        processor.dispatch(msg.topic, msg.payload.decode("utf-8"))
    except Exception as e:
        log.error(f"Error en {msg.topic}: {e}", exc_info=True)

# ============================================================
# API HTTP
# ============================================================

api = Flask(__name__)

@api.route("/api/config/<device_id>", methods=["GET"])
def get_config(device_id):
    rows = db.fetchall(
        "SELECT pump_power_kw,cost_kwh,cost_water_m3,target_recovery,"
        "target_efficiency,daily_target_liters,friendly_name,location "
        "FROM device_config WHERE device_id=%s",
        (device_id,)
    )
    if not rows:
        return jsonify({"error": "device not found"}), 404
    r = rows[0]
    return jsonify({
        "pump_power_kw": r[0], "cost_kwh": r[1], "cost_water_m3": r[2],
        "target_recovery": r[3], "target_efficiency": r[4],
        "daily_target_liters": r[5], "friendly_name": r[6], "location": r[7],
    })

@api.route("/api/config/<device_id>", methods=["POST"])
def set_config(device_id):
    data = request.json or {}
    db.execute(
        "INSERT INTO device_config "
        "(device_id,pump_power_kw,cost_kwh,cost_water_m3,"
        "target_recovery,target_efficiency,daily_target_liters,friendly_name,location,updated_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()) "
        "ON CONFLICT (device_id) DO UPDATE SET "
        "pump_power_kw=EXCLUDED.pump_power_kw, cost_kwh=EXCLUDED.cost_kwh, "
        "cost_water_m3=EXCLUDED.cost_water_m3, target_recovery=EXCLUDED.target_recovery, "
        "target_efficiency=EXCLUDED.target_efficiency, "
        "daily_target_liters=EXCLUDED.daily_target_liters, "
        "friendly_name=EXCLUDED.friendly_name, location=EXCLUDED.location, updated_at=NOW()",
        (device_id,
         data.get("pump_power_kw", 0.75),     data.get("cost_kwh", 0.12),
         data.get("cost_water_m3", 0.80),     data.get("target_recovery", 0.65),
         data.get("target_efficiency", 0.92), data.get("daily_target_liters", 0),
         data.get("friendly_name", ""),       data.get("location", "")),
    )
    KPIEngine.invalidate_config(device_id)
    return jsonify({"status": "ok"})

@api.route("/api/baseline/<device_id>", methods=["GET"])
def get_baseline(device_id):
    cols = []
    for field in BASELINE_FIELDS:
        cols += [f"{field}_learned", f"{field}_manual", f"{field}_source"]
    cols += ["learned_at", "session_id",
             "efficiency_mean", "efficiency_std",
             "recovery_mean", "recovery_std",
             "flow_perm_mean", "flow_perm_std",
             "delta_pressure_mean", "delta_pressure_std"]

    rows = db.fetchall(
        f"SELECT {', '.join(cols)} FROM device_baseline WHERE device_id = %s",
        (device_id,)
    )

    thresholds = []
    stats_data = {}

    if rows:
        row = rows[0]
        for i, field in enumerate(BASELINE_FIELDS):
            learned = row[i * 3]
            manual  = row[i * 3 + 1]
            source  = row[i * 3 + 2] or "learned"
            active  = (manual if source == "manual" and manual is not None else
                       learned if learned is not None else
                       THRESHOLDS[BaselineCache._FALLBACK[field]])
            thresholds.append({
                "field": field, "learned": learned, "manual": manual,
                "source": source, "active_value": active,
                "fallback_used": learned is None and manual is None,
            })
        offset = len(BASELINE_FIELDS) * 3
        stats_data = {
            "learned_at":          str(row[offset]),     "session_id":      row[offset+1],
            "efficiency_mean":     row[offset+2],         "efficiency_std":  row[offset+3],
            "recovery_mean":       row[offset+4],         "recovery_std":    row[offset+5],
            "flow_perm_mean":      row[offset+6],         "flow_perm_std":   row[offset+7],
            "delta_pressure_mean": row[offset+8],         "delta_pressure_std": row[offset+9],
        }
    else:
        for field in BASELINE_FIELDS:
            thresholds.append({
                "field": field, "learned": None, "manual": None, "source": "learned",
                "active_value": THRESHOLDS[BaselineCache._FALLBACK[field]], "fallback_used": True,
            })

    return jsonify({"device_id": device_id, "has_baseline": bool(rows),
                    "thresholds": thresholds, "stats": stats_data})

@api.route("/api/baseline/<device_id>", methods=["POST"])
def set_baseline(device_id):
    data = request.json or {}
    rows = db.fetchall("SELECT device_id FROM devices WHERE device_id=%s", (device_id,))
    if not rows:
        return jsonify({"error": f"Device '{device_id}' not found"}), 404

    db.execute(
        "INSERT INTO device_baseline (device_id, learned_at) VALUES (%s, NOW()) "
        "ON CONFLICT (device_id) DO NOTHING",
        (device_id,)
    )

    updated_values  = {}
    updated_sources = {}

    for field in BASELINE_FIELDS:
        if field in data and data[field] is not None:
            val = float(data[field])
            db.execute(
                f"UPDATE device_baseline SET {field}_manual=%s, {field}_source='manual' "
                f"WHERE device_id=%s",
                (val, device_id)
            )
            updated_values[field]  = val
            updated_sources[field] = "manual"

        source_key = f"{field}_source"
        if source_key in data:
            source = data[source_key]
            if source not in ("learned", "manual"):
                return jsonify({"error": "source debe ser 'learned' o 'manual'"}), 400
            db.execute(
                f"UPDATE device_baseline SET {field}_source=%s WHERE device_id=%s",
                (source, device_id)
            )
            updated_sources[field] = source

    if not updated_values and not updated_sources:
        return jsonify({"error": "No se recibió ningún campo válido.",
                        "allowed_fields": BASELINE_FIELDS}), 400

    BaselineCache.invalidate(device_id)
    return jsonify({
        "status": "ok", "device_id": device_id,
        "updated_values": updated_values, "updated_sources": updated_sources,
        "note": "Activo en el próximo ciclo de diagnóstico.",
    })

@api.route("/api/learn/start/<device_id>", methods=["POST"])
def start_learn(device_id):
    duration   = (request.json or {}).get("duration_minutes", 30)
    session_id = learn_engine.start(device_id, duration)
    return jsonify({"status": "started", "session_id": session_id,
                    "duration_minutes": duration})

@api.route("/api/learn/status/<device_id>", methods=["GET"])
def learn_status(device_id):
    if learn_engine.is_active(device_id):
        s = learn_engine._active[device_id]
        elapsed = int(time.time() - s["started_at"])
        return jsonify({
            "status":        "RUNNING",
            "samples":       len(s["samples"]),
            "elapsed_sec":   elapsed,
            "remaining_sec": max(0, s["duration_sec"] - elapsed),
            "progress_pct":  round(elapsed / s["duration_sec"] * 100, 1),
        })
    rows = db.fetchall(
        "SELECT status,samples,finished_at FROM learning_sessions "
        "WHERE device_id=%s ORDER BY started_at DESC LIMIT 1",
        (device_id,)
    )
    if rows:
        return jsonify({"status": rows[0][0], "samples": rows[0][1],
                        "finished_at": str(rows[0][2])})
    return jsonify({"status": "NEVER_RUN"})

@api.route("/api/learn/cancel/<device_id>", methods=["POST"])
def cancel_learn(device_id):
    learn_engine.cancel(device_id)
    return jsonify({"status": "cancelled"})

@api.route("/api/status/<device_id>", methods=["GET"])
def get_status(device_id):
    rows = db.fetchall(
        """
        SELECT state, last_severity, last_diag_code, last_diag_message, last_action,
               flow_perm_lpm, pressure_membrane, recovery, efficiency,
               last_seen, online,
               health_status, health_code, health_message, health_action, health_updated_at,
               biz_liters_today, biz_target_liters, biz_fulfillment_pct,
               biz_waste_liters_today, biz_waste_pct,
               biz_risk_level, biz_risk_score,
               biz_degradation_pct, biz_degradation_days, biz_degradation_label,
               biz_health_age_hours
        FROM device_status WHERE device_id=%s
        """,
        (device_id,)
    )
    if not rows:
        return jsonify({"error": "device not found"}), 404
    r = rows[0]
    return jsonify({
        "state": r[0],         "last_severity": r[1],
        "diag_code": r[2],     "diag_message": r[3],   "diag_action": r[4],
        "flow_perm_lpm": r[5], "pressure": r[6],
        "recovery": r[7],      "efficiency": r[8],
        "last_seen": str(r[9]), "online": r[10],
        "health": {
            "status": r[11],   "code": r[12],
            "message": r[13],  "action": r[14],
            "updated_at": str(r[15]),
            "age_hours": r[26],
        },
        "business": {
            "liters_today": r[16],       "target_liters": r[17],
            "fulfillment_pct": r[18],    "waste_liters_today": r[19],
            "waste_pct": r[20],          "risk_level": r[21],
            "risk_score": r[22],         "degradation_pct": r[23],
            "degradation_days": r[24],   "degradation_label": r[25],
        },
    })

@api.route("/api/business/<device_id>", methods=["GET"])
def get_business_history(device_id):
    """Historial de métricas de negocio (últimos 30 días)."""
    days = int(request.args.get("days", 30))
    rows = db.fetchall(
        """
        SELECT day, liters_produced, liters_rejected,
               daily_target_liters, fulfillment_pct, waste_pct,
               avg_efficiency, avg_recovery, risk_level, estimated_cost
        FROM business_metrics
        WHERE device_id=%s AND day >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY day DESC
        """,
        (device_id, days)
    )
    return jsonify([{
        "day": str(r[0]),        "liters_produced": r[1],
        "liters_rejected": r[2], "target": r[3],
        "fulfillment_pct": r[4], "waste_pct": r[5],
        "avg_efficiency": r[6],  "avg_recovery": r[7],
        "risk_level": r[8],      "estimated_cost": r[9],
    } for r in rows])


def _start_api():
    api.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)

# ============================================================
# MAIN
# ============================================================

def main():
    log.info("🚀 Fyntek RO Backend v3.3 iniciando...")

    for attempt in range(10):
        try:
            db.connect()
            break
        except Exception as e:
            log.warning(f"DB no disponible ({attempt+1}/10): {e}")
            time.sleep(3)
    else:
        log.critical("No se pudo conectar a la DB. Abortando.")
        return

    api_thread = threading.Thread(target=_start_api, daemon=True)
    api_thread.start()
    log.info("✅ API HTTP en puerto 8080")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        log.critical(f"No se pudo conectar al broker: {e}")
        return

    log.info("✅ Escuchando MQTT...")
    client.loop_forever()


if __name__ == "__main__":
    main()
