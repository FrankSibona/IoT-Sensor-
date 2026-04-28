"""
Fyntek RO Backend - worker.py v2.0
====================================
Backend de monitoreo para sistemas de ósmosis inversa.


Arquitectura:
  MQTT → MessageProcessor → KPIEngine + DiagnosticEngine → DB + AlertManager


Tópicos consumidos (del firmware comms.cpp):
  fyntek/{device_id}/process    → cada 1 segundo
  fyntek/{device_id}/quality    → cada 10 segundos
  fyntek/{device_id}/state      → on-change
  fyntek/{device_id}/inputs     → on-change
  fyntek/{device_id}/outputs    → on-change
  fyntek/{device_id}/heartbeat  → cada 30 segundos


NOTA CRÍTICA sobre TDS:
  El firmware envía los valores como 'tds_in_ppm' y 'tds_out_ppm',
  pero en realidad son voltajes ADC (0-5V). La fórmula en sensors.cpp:
    float t1 = (analogRead(PIN_TDS1) / 4095.0) * 5.0;
  La conversión real a PPM depende de la calibración del sensor.
  La eficiencia de membrana se calcula como ratio (no necesita ppm absoluto),
  pero los valores raw en DB son voltajes, no PPM.
"""


import json
import logging
import os
import time
from collections import deque, defaultdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any


import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.pool
import requests


# ============================================================
# CONFIGURACIÓN
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


# --- Base de datos ---
DB_HOST = os.getenv("DB_HOST", "ro-postgres")
DB_NAME = os.getenv("DB_NAME", "iot_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = int(os.getenv("DB_PORT", "5432"))


# --- MQTT ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "ro-mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "kairox")
MQTT_PASS = os.getenv("MQTT_PASS", "admin0102")
MQTT_TOPIC = "fyntek/#"


# --- Telegram ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
# Directorio: device_id → chat_id de Telegram
# Se puede mover a base de datos a futuro para ser 100% dinámico
TELEGRAM_DIRECTORY: Dict[str, str] = {
    "ESP32_DEFAULT": os.getenv("TELEGRAM_ADMIN_CHAT", ""),
    # Agregar dispositivos: "ESP32_XXXX": "chat_id"
}
TELEGRAM_ADMIN_CHAT = os.getenv("TELEGRAM_ADMIN_CHAT", "")


# --- Umbrales de diagnóstico (configurables por env o futura tabla DB) ---
THRESHOLDS = {
    "pressure_max_bar": float(os.getenv("THRESH_PRESSURE_MAX", "9.0")),
    "pressure_low_bar": float(os.getenv("THRESH_PRESSURE_LOW", "2.0")),
    "efficiency_warning": float(os.getenv("THRESH_EFF_WARNING", "0.85")),
    "efficiency_critical": float(os.getenv("THRESH_EFF_CRITICAL", "0.70")),
    "recovery_min": float(os.getenv("THRESH_RECOVERY_MIN", "0.25")),
    "recovery_max": float(os.getenv("THRESH_RECOVERY_MAX", "0.85")),
    "flow_perm_min_lpm": float(os.getenv("THRESH_FLOW_MIN", "0.3")),
    "alert_cooldown_sec": int(os.getenv("ALERT_COOLDOWN", "300")),
    # Ventana de tendencias: cuántos puntos de métricas usar para detectar tendencia
    "trend_window": int(os.getenv("TREND_WINDOW", "30")),
    # Pendiente mínima para considerar "sube sostenidamente" (unidades/muestra)
    "pressure_trend_threshold": float(os.getenv("TREND_PRESSURE", "0.02")),
    "efficiency_trend_threshold": float(os.getenv("TREND_EFFICIENCY", "-0.005")),
}


# ============================================================
# DATABASE POOL
# ============================================================


class DatabasePool:
    """
    Pool de conexiones PostgreSQL.
    Usamos un pool simple para reutilizar conexiones entre mensajes.
    Con 1 msg/seg de proceso, abrir una conexión por mensaje rompería el DB.
    """


    def __init__(self):
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


    def connect(self):
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )
        log.info("✅ DB pool conectado")


    def execute(self, sql: str, params: tuple = ()):
        """Ejecuta un INSERT/UPDATE sin retorno."""
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
        """Ejecuta un SELECT y retorna filas."""
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


db = DatabasePool()


# ============================================================
# VALIDACIÓN Y UTILIDADES
# ============================================================


STATE_MAP = {
    "IDLE": 0, "STARTING": 1, "PRODUCING": 2,
    "FLUSHING": 3, "STOPPING": 4, "FAULT": 5,
}


def ts_to_utc(ts_unix: Any) -> Optional[datetime]:
    """Convierte timestamp UNIX del dispositivo a datetime UTC."""
    try:
        val = int(ts_unix)
        if val <= 0:
            return None
        return datetime.fromtimestamp(val, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def validate_float(value: Any, vmin: float = -1000, vmax: float = 1000) -> Optional[float]:
    """Valida y normaliza un valor numérico. Retorna None si es inválido."""
    try:
        v = float(value)
        if v < vmin or v > vmax:
            return None
        return v
    except (TypeError, ValueError):
        return None


def validate_bool(value: Any) -> Optional[bool]:
    try:
        if isinstance(value, bool):
            return value
        return bool(int(value))
    except (TypeError, ValueError):
        return None


# ============================================================
# KPI ENGINE
# ============================================================


class KPIEngine:
    """
    Calcula KPIs de osmosis inversa a partir de los datos de proceso y calidad.


    Los KPIs solo se calculan cuando el equipo está PRODUCIENDO
    y con datos suficientes en ambos tópicos.
    """


    # Cache de último dato de quality por device
    _last_quality: Dict[str, Dict] = {}


    @classmethod
    def update_quality_cache(cls, device_id: str, quality_data: Dict):
        cls._last_quality[device_id] = quality_data


    @classmethod
    def compute(cls, device_id: str, process: Dict, state: str) -> Optional[Dict]:
        """
        Retorna dict de métricas o None si no hay datos suficientes.
        Solo calcula en estado PRODUCING para evitar ruido.
        """
        # Solo calculamos mientras produce
        if state not in ("PRODUCING", "STARTING"):
            return None


        flow_p = validate_float(process.get("flow_perm_lpm"), 0, 100)
        flow_r = validate_float(process.get("flow_rechazo_lpm"), 0, 100)
        p_mem  = validate_float(process.get("pressure_membrane_bar"), 0, 50)
        p_brine = validate_float(process.get("pressure_brine_bar"), 0, 50)


        if flow_p is None or flow_r is None:
            return None


        total_flow = flow_p + flow_r


        # --- Recovery (recuperación) ---
        # recovery bajo → desperdicio; alto → riesgo fouling
        recovery = (flow_p / total_flow) if total_flow > 0.01 else None


        # --- Rejection ratio ---
        rejection_ratio = (flow_r / flow_p) if flow_p > 0.01 else None


        # --- Delta de presión (indicador de fouling) ---
        delta_p = None
        if p_mem is not None and p_brine is not None:
            delta_p = p_mem - p_brine


        # --- Eficiencia de membrana (requiere quality) ---
        # NOTA: tds_in_raw y tds_out_raw son VOLTAJES (0-5V), no PPM.
        # La eficiencia como ratio es válida igual.
        efficiency = None
        quality = cls._last_quality.get(device_id, {})
        tds_in  = validate_float(quality.get("tds_in_raw"), 0, 5)
        tds_out = validate_float(quality.get("tds_out_raw"), 0, 5)


        if tds_in is not None and tds_in > 0.05:
            efficiency = 1.0 - (tds_out / tds_in) if tds_out is not None else None


        return {
            "recovery":           recovery,
            "efficiency":         efficiency,
            "rejection_ratio":    rejection_ratio,
            "delta_pressure_bar": delta_p,
            "flow_perm_lpm":      flow_p,
            "flow_rechazo_lpm":   flow_r,
            "tds_in_raw":         tds_in,
            "tds_out_raw":        tds_out,
        }


# ============================================================
# TREND ANALYZER
# ============================================================


class TrendAnalyzer:
    """
    Detecta tendencias en las métricas usando regresión lineal simple
    sobre los últimos N puntos en memoria.


    Usamos un buffer en memoria para no hacer un SELECT por cada mensaje.
    El buffer se reconstruye al iniciar desde la DB (TODO: al reconectar MQTT).
    """


    def __init__(self, window: int = 30):
        self.window = window
        # device_id → deque de valores
        self._buffers: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: {
                "pressure": deque(maxlen=window),
                "efficiency": deque(maxlen=window),
                "flow_perm": deque(maxlen=window),
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
        """
        Calcula la pendiente (unidades/muestra) de la variable.
        Positivo = sube, Negativo = baja.
        Retorna None si no hay suficientes datos.
        """
        values = list(self._buffers[device_id][variable])
        n = len(values)
        if n < 5:
            return None
        # Regresión lineal mínima
        x_mean = (n - 1) / 2
        y_mean = sum(values) / n
        num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
        den = sum((i - x_mean) ** 2 for i in range(n))
        return num / den if den > 0 else 0.0


    def get_trends(self, device_id: str) -> Dict:
        return {
            "pressure_slope":    self.slope(device_id, "pressure"),
            "efficiency_slope":  self.slope(device_id, "efficiency"),
            "flow_slope":        self.slope(device_id, "flow_perm"),
        }


trend_analyzer = TrendAnalyzer(window=THRESHOLDS["trend_window"])


# ============================================================
# DIAGNOSTIC ENGINE
# ============================================================


class DiagnosticResult:
    def __init__(self, severity: str, code: str, message: str, details: Dict):
        self.severity = severity
        self.code = code
        self.message = message
        self.details = details


    def __repr__(self):
        return f"[{self.severity}] {self.code}: {self.message}"




class DiagnosticEngine:
    """
    Motor de diagnóstico determinístico basado en reglas compuestas.


    Las reglas compuestas son el diferenciador clave:
    NO alertamos por variables aisladas, sino por combinaciones
    que tienen significado físico concreto.


    Cada regla retorna un DiagnosticResult o None.
    """


    def run(
        self,
        device_id: str,
        process: Dict,
        metrics: Optional[Dict],
        state: str,
        inputs: Optional[Dict],
        trends: Optional[Dict],
    ) -> DiagnosticResult:
        """
        Evalúa todas las reglas en orden de prioridad.
        Retorna el diagnóstico más grave encontrado, o OK si todo está bien.
        """


        # 1. Fallas de hardware (máxima prioridad)
        fault = self._check_faults(state, inputs, process)
        if fault:
            return fault


        # 2. Condiciones críticas de operación
        if metrics:
            critical = self._check_critical(metrics, process)
            if critical:
                return critical


            # 3. Diagnósticos compuestos (el corazón del producto)
            compound = self._check_compound(metrics, process, trends)
            if compound:
                return compound


            # 4. Advertencias
            warning = self._check_warnings(metrics, process, trends)
            if warning:
                return warning


        return DiagnosticResult("OK", "NORMAL", "Operando normalmente", {})


    # --- REGLAS DE FALLO ---


    def _check_faults(self, state, inputs, process) -> Optional[DiagnosticResult]:


        if state == "FAULT":
            # Intentamos dar más contexto
            crudo = (inputs or {}).get("crudo_ok", True)
            retry = process.get("retry_count", 0)
            details = {"state": state, "crudo_ok": crudo, "retry": retry}
            if not crudo:
                return DiagnosticResult(
                    "CRITICAL", "FAULT_NO_WATER",
                    "FALLA: Sin agua de crudo. El sistema no puede arrancar.",
                    details
                )
            return DiagnosticResult(
                "CRITICAL", "FAULT_SYSTEM",
                f"FALLA DEL SISTEMA: retries agotados ({retry}). Verificar presión y agua.",
                details
            )


        if inputs:
            if not inputs.get("crudo_ok", True) and state not in ("IDLE", "FLUSHING"):
                return DiagnosticResult(
                    "CRITICAL", "NO_RAW_WATER",
                    "Sin agua de crudo mientras el equipo intenta operar.",
                    {"crudo_ok": False, "state": state}
                )


        return None


    # --- REGLAS CRÍTICAS ---


    def _check_critical(self, metrics, process) -> Optional[DiagnosticResult]:
        p_mem = validate_float(process.get("pressure_membrane_bar"), 0, 50)


        if p_mem is not None and p_mem > THRESHOLDS["pressure_max_bar"]:
            return DiagnosticResult(
                "CRITICAL", "HIGH_PRESSURE",
                f"Presión crítica: {p_mem:.1f} bar (máx {THRESHOLDS['pressure_max_bar']} bar). "
                "Verificar válvulas y membrana.",
                {"pressure_membrane_bar": p_mem}
            )


        eff = metrics.get("efficiency")
        if eff is not None and eff < THRESHOLDS["efficiency_critical"]:
            return DiagnosticResult(
                "CRITICAL", "CRITICAL_EFFICIENCY",
                f"Eficiencia crítica de membrana: {eff*100:.1f}%. "
                "Posible rotura o fouling severo.",
                {"efficiency": eff}
            )


        return None


    # --- REGLAS COMPUESTAS (el diferenciador) ---


    def _check_compound(self, metrics, process, trends) -> Optional[DiagnosticResult]:
        p_mem  = validate_float(process.get("pressure_membrane_bar"), 0, 50)
        flow_p = metrics.get("flow_perm_lpm")
        eff    = metrics.get("efficiency")
        rec    = metrics.get("recovery")
        delta_p = metrics.get("delta_pressure_bar")


        # ⚠️ MEMBRANA TAPADA: presión alta + bajo caudal de permeado
        if (p_mem is not None and p_mem > THRESHOLDS["pressure_max_bar"] * 0.85 and
                flow_p is not None and flow_p < THRESHOLDS["flow_perm_min_lpm"]):
            return DiagnosticResult(
                "WARNING", "MEMBRANE_FOULING",
                f"Posible ensuciamiento de membrana: presión alta ({p_mem:.1f} bar) "
                f"con bajo caudal de permeado ({flow_p:.2f} L/min).",
                {"pressure_membrane_bar": p_mem, "flow_perm_lpm": flow_p}
            )


        # ⚠️ SCALING: baja eficiencia + recovery alto
        # Indica que el rechazo está concentrado y precipitando sales
        if (eff is not None and eff < THRESHOLDS["efficiency_warning"] and
                rec is not None and rec > THRESHOLDS["recovery_max"]):
            return DiagnosticResult(
                "WARNING", "MEMBRANE_SCALING",
                f"Posible incrustación (scaling): eficiencia baja ({eff*100:.1f}%) "
                f"con recovery alto ({rec*100:.1f}%). Revisar antiscalante y frecuencia de flush.",
                {"efficiency": eff, "recovery": rec}
            )


        # ⚠️ DEGRADACIÓN QUÍMICA: baja eficiencia + presión normal
        # La membrana está degradada químicamente, no por obstrucción
        if (eff is not None and eff < THRESHOLDS["efficiency_warning"] and
                p_mem is not None and
                THRESHOLDS["pressure_low_bar"] < p_mem < THRESHOLDS["pressure_max_bar"] * 0.8):
            return DiagnosticResult(
                "WARNING", "MEMBRANE_DEGRADED",
                f"Posible degradación química de membrana: eficiencia baja ({eff*100:.1f}%) "
                f"con presión normal ({p_mem:.1f} bar).",
                {"efficiency": eff, "pressure_membrane_bar": p_mem}
            )


        # ⚠️ FOULING PROGRESIVO: tendencia ascendente en delta de presión
        if trends:
            p_slope = trends.get("pressure_slope")
            if (p_slope is not None and
                    p_slope > THRESHOLDS["pressure_trend_threshold"] and
                    delta_p is not None and delta_p > 1.0):
                return DiagnosticResult(
                    "WARNING", "PROGRESSIVE_FOULING",
                    f"Fouling progresivo detectado: la diferencia de presión sube "
                    f"sostenidamente (+{p_slope:.4f} bar/muestra). Planificar limpieza.",
                    {"delta_pressure_bar": delta_p, "pressure_slope": p_slope}
                )


        return None


    # --- ADVERTENCIAS SIMPLES ---


    def _check_warnings(self, metrics, process, trends) -> Optional[DiagnosticResult]:
        eff = metrics.get("efficiency")
        rec = metrics.get("recovery")
        flow_p = metrics.get("flow_perm_lpm")
        p_mem  = validate_float(process.get("pressure_membrane_bar"), 0, 50)


        if eff is not None and eff < THRESHOLDS["efficiency_warning"]:
            return DiagnosticResult(
                "WARNING", "LOW_EFFICIENCY",
                f"Eficiencia de membrana baja: {eff*100:.1f}%. "
                "Revisar TDS, estado de membrana o necesidad de limpieza.",
                {"efficiency": eff}
            )


        if rec is not None and rec < THRESHOLDS["recovery_min"]:
            return DiagnosticResult(
                "WARNING", "LOW_RECOVERY",
                f"Recovery bajo: {rec*100:.1f}%. Alta relación de rechazo. "
                "Verificar válvulas y configuración de caudales.",
                {"recovery": rec}
            )


        if flow_p is not None and flow_p < THRESHOLDS["flow_perm_min_lpm"]:
            return DiagnosticResult(
                "WARNING", "LOW_PERMEATE_FLOW",
                f"Caudal de permeado bajo: {flow_p:.2f} L/min. "
                "Verificar presión de entrada y estado de membrana.",
                {"flow_perm_lpm": flow_p}
            )


        if p_mem is not None and p_mem < THRESHOLDS["pressure_low_bar"]:
            return DiagnosticResult(
                "WARNING", "LOW_PRESSURE",
                f"Presión de membrana baja: {p_mem:.1f} bar. "
                "Verificar bomba y válvulas.",
                {"pressure_membrane_bar": p_mem}
            )


        # Tendencia de eficiencia descendente
        if trends:
            eff_slope = trends.get("efficiency_slope")
            if (eff_slope is not None and
                    eff_slope < THRESHOLDS["efficiency_trend_threshold"]):
                return DiagnosticResult(
                    "WARNING", "DECLINING_EFFICIENCY",
                    f"Eficiencia de membrana en tendencia descendente "
                    f"({eff_slope*100:.3f}%/muestra). Programar revisión preventiva.",
                    {"efficiency_slope": eff_slope}
                )


        return None




diagnostic_engine = DiagnosticEngine()


# ============================================================
# ALERT MANAGER
# ============================================================


class AlertManager:
    """
    Ahora resuelve el chat_id desde la tabla 'devices' en DB.
    Si el device_id no está registrado, usa el chat del admin.
    El cooldown sigue siendo independiente por dispositivo.
    """


    def __init__(self):
        self._last_alert: Dict[str, float] = {}
        self._chat_cache: Dict[str, Optional[str]] = {}  # cache en memoria


    def _get_chat(self, device_id: str) -> Optional[str]:
        # Cache para no ir a DB en cada mensaje
        if device_id in self._chat_cache:
            return self._chat_cache[device_id]


        rows = db.fetchall(
            "SELECT telegram_chat_id FROM devices WHERE device_id = %s",
            (device_id,)
        )
        chat_id = rows[0][0] if rows and rows[0][0] else TELEGRAM_ADMIN_CHAT
        self._chat_cache[device_id] = chat_id
        return chat_id


    def invalidate_cache(self, device_id: str):
        """Llamar si actualizás el chat_id de un dispositivo en DB."""
        self._chat_cache.pop(device_id, None)


    def process(self, device_id: str, diag: DiagnosticResult):
        if diag.severity == "OK":
            return


        now = time.time()
        cooldown = THRESHOLDS["alert_cooldown_sec"]
        if (now - self._last_alert.get(device_id, 0)) < cooldown:
            return


        chat_id = self._get_chat(device_id)
        if not chat_id or not TELEGRAM_TOKEN:
            log.warning(f"Sin chat_id configurado para {device_id}")
            return


        icon = "🚨" if diag.severity == "CRITICAL" else "⚠️"
        msg = (
            f"{icon} *FYNTEK [{device_id}]*\n"
            f"*{diag.code}*\n"
            f"{diag.message}"
        )


        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            resp = requests.post(
                url,
                json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
                timeout=5,
            )
            if resp.ok:
                self._last_alert[device_id] = now
                log.info(f"📱 Telegram enviado a {device_id}: {diag.code}")
            else:
                log.error(f"Telegram error {resp.status_code}: {resp.text}")
        except Exception as e:
            log.error(f"Telegram excepción: {e}")




alert_manager = AlertManager()


# ============================================================
# STATE TRACKER (en memoria)
# ============================================================


class DeviceStateTracker:
    """
    Mantiene el último estado conocido de cada dispositivo en memoria.
    Necesario para que el DiagnosticEngine tenga contexto cruzado
    entre tópicos (process, quality, state, inputs).
    """


    def __init__(self):
        self._state:   Dict[str, str]  = defaultdict(lambda: "UNKNOWN")
        self._inputs:  Dict[str, Dict] = {}
        self._process: Dict[str, Dict] = {}


    def update_state(self, device_id: str, state: str):
        self._state[device_id] = state


    def update_inputs(self, device_id: str, data: Dict):
        self._inputs[device_id] = data


    def update_process(self, device_id: str, data: Dict):
        self._process[device_id] = data


    def get_state(self, device_id: str) -> str:
        return self._state.get(device_id, "UNKNOWN")


    def get_inputs(self, device_id: str) -> Optional[Dict]:
        return self._inputs.get(device_id)


    def get_process(self, device_id: str) -> Optional[Dict]:
        return self._process.get(device_id)




tracker = DeviceStateTracker()


# ============================================================
# MESSAGE PROCESSOR
# ============================================================


class MessageProcessor:
    """
    Procesa mensajes MQTT por tópico.
    Cada handler:
      1. Valida y parsea el payload
      2. Persiste en la tabla correspondiente
      3. Actualiza el tracker en memoria
      4. Si es 'process': calcula KPIs + diagnóstico + alerta
    """


    def dispatch(self, topic: str, payload: str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            log.warning(f"JSON inválido en {topic}: {e}")
            return


        device_id = data.get("device_id", "unknown")
        ts_raw = data.get("ts")
        timestamp = ts_to_utc(ts_raw)


        if timestamp is None:
            log.warning(f"[{device_id}] timestamp inválido ({ts_raw}), descartando")
            return


        # Routing por sufijo del tópico
        parts = topic.split("/")  # fyntek / {device_id} / {subtopic}
        if len(parts) < 3:
            return
        subtopic = parts[2]


        handlers = {
            "process":   self._handle_process,
            "quality":   self._handle_quality,
            "state":     self._handle_state,
            "inputs":    self._handle_inputs,
            "outputs":   self._handle_outputs,
            "heartbeat": self._handle_heartbeat,
        }


        handler = handlers.get(subtopic)
        if handler:
            handler(device_id, timestamp, data)
        else:
            log.debug(f"Tópico sin handler: {subtopic}")


    # ---- PROCESS ----


    def _handle_process(self, device_id: str, timestamp: datetime, data: Dict):
        flow_p   = validate_float(data.get("flow_perm_lpm"), 0, 100)
        flow_r   = validate_float(data.get("flow_rechazo_lpm"), 0, 100)
        p_mem    = validate_float(data.get("pressure_membrane_bar"), 0, 50)
        p_brine  = validate_float(data.get("pressure_brine_bar"), 0, 50)
        vol_p    = validate_float(data.get("volume_perm_l"), 0, 1e7)
        vol_r    = validate_float(data.get("volume_rechazo_l"), 0, 1e7)
        fw       = data.get("fw_version", "")


        db.execute(
            """
            INSERT INTO telemetry_process
              (time, device_id, flow_perm_lpm, flow_rechazo_lpm,
               pressure_membrane_bar, pressure_brine_bar,
               volume_perm_l, volume_rechazo_l, fw_version)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (timestamp, device_id, flow_p, flow_r, p_mem, p_brine, vol_p, vol_r, fw),
        )


        tracker.update_process(device_id, data)


        # Calculamos KPIs + diagnóstico solo si estamos produciendo
        self._run_analytics(device_id, timestamp, data)


    # ---- QUALITY ----


    def _handle_quality(self, device_id: str, timestamp: datetime, data: Dict):
        # NOTA: los campos del firmware se llaman tds_in_ppm y tds_out_ppm
        # pero en realidad son voltajes (0-5V). Ver nota en el módulo.
        tds_in  = validate_float(data.get("tds_in_ppm"), 0, 5)
        tds_out = validate_float(data.get("tds_out_ppm"), 0, 5)


        db.execute(
            "INSERT INTO telemetry_quality (time, device_id, tds_in_raw, tds_out_raw, fw_version) "
            "VALUES (%s,%s,%s,%s,%s)",
            (timestamp, device_id, tds_in, tds_out, data.get("fw_version", "")),
        )


        KPIEngine.update_quality_cache(device_id, {"tds_in_raw": tds_in, "tds_out_raw": tds_out})


    # ---- STATE ----


    def _handle_state(self, device_id: str, timestamp: datetime, data: Dict):
        state = data.get("state", "UNKNOWN")
        state_num = STATE_MAP.get(state, -1)
        running = validate_bool(data.get("running"))
        retry = data.get("retry", 0)


        db.execute(
            "INSERT INTO telemetry_state (time, device_id, state, state_numeric, running, retry_count) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (timestamp, device_id, state, state_num, running, retry),
        )


        tracker.update_state(device_id, state)
        log.info(f"[{device_id}] Estado → {state} (retry={retry})")


        # Si entra en FAULT, disparamos diagnóstico inmediato
        if state == "FAULT":
            self._run_analytics(device_id, timestamp, tracker.get_process(device_id) or {})


    # ---- INPUTS ----


    def _handle_inputs(self, device_id: str, timestamp: datetime, data: Dict):
        demand    = validate_bool(data.get("demand"))
        crudo_ok  = validate_bool(data.get("crudo_ok"))
        dose_ok   = validate_bool(data.get("dose_ok"))
        presostato = validate_bool(data.get("presostato"))
        reserva1  = validate_bool(data.get("reserva1"))
        reserva2  = validate_bool(data.get("reserva2"))


        db.execute(
            "INSERT INTO telemetry_inputs (time, device_id, demand, crudo_ok, dose_ok, presostato, reserva1, reserva2) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (timestamp, device_id, demand, crudo_ok, dose_ok, presostato, reserva1, reserva2),
        )


        tracker.update_inputs(device_id, {
            "demand": demand, "crudo_ok": crudo_ok, "dose_ok": dose_ok,
            "presostato": presostato, "reserva1": reserva1, "reserva2": reserva2,
        })


    # ---- OUTPUTS ----


    def _handle_outputs(self, device_id: str, timestamp: datetime, data: Dict):
        db.execute(
            "INSERT INTO telemetry_outputs "
            "(time, device_id, pump_low, pump_high, pump_inlet, pump_dose, valve_flush, valve_inlet) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                timestamp, device_id,
                validate_bool(data.get("pump_low")),
                validate_bool(data.get("pump_high")),
                validate_bool(data.get("pump_inlet")),
                validate_bool(data.get("pump_dose")),
                validate_bool(data.get("valve_flush")),
                validate_bool(data.get("valve_inlet")),
            ),
        )


   # ---- FUNCION AUTOREGISTRO ----


def _auto_register_device(self, device_id: str, fw_version: str = ""):
    """
    Registra el dispositivo en la tabla 'devices' si no existe.
    No sobreescribe datos que el operador haya cargado manualmente.
    """
    db.execute(
        """
        INSERT INTO devices (device_id, fw_version, registered_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (device_id) DO UPDATE
            SET fw_version = EXCLUDED.fw_version
        """,
        (device_id, fw_version)
    )
    log.info(f"[{device_id}] Dispositivo registrado/actualizado en DB")


  # ---- HEARTBEAT ----


def _handle_heartbeat(self, device_id: str, timestamp: datetime, data: Dict):
    # Auto-registro: silencioso si ya existe
    self._auto_register_device(device_id, data.get("fw_version", ""))


    db.execute(
        """
        INSERT INTO device_status (device_id, last_seen, online)
        VALUES (%s, %s, TRUE)
        ON CONFLICT (device_id) DO UPDATE
          SET last_seen = EXCLUDED.last_seen,
              online    = TRUE
        """,
        (device_id, timestamp),
    )
    log.debug(f"[{device_id}] Heartbeat recibido")


    # ---- ANALYTICS PIPELINE ----


    def _run_analytics(self, device_id: str, timestamp: datetime, process_data: Dict):
        """
        Pipeline: KPIs → Tendencias → Diagnóstico → Alerta → Persistencia
        """
        state  = tracker.get_state(device_id)
        inputs = tracker.get_inputs(device_id)


        # 1. Calcular KPIs
        metrics = KPIEngine.compute(device_id, process_data, state)


        if metrics:
            # 2. Actualizar buffer de tendencias
            trend_analyzer.add_metrics(device_id, metrics)
            trends = trend_analyzer.get_trends(device_id)


            # 3. Persistir métricas
            db.execute(
                """
                INSERT INTO metrics
                  (time, device_id, recovery, efficiency, rejection_ratio,
                   delta_pressure_bar, flow_perm_lpm, flow_rechazo_lpm, tds_in_raw, tds_out_raw)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    timestamp, device_id,
                    metrics["recovery"], metrics["efficiency"],
                    metrics["rejection_ratio"], metrics["delta_pressure_bar"],
                    metrics["flow_perm_lpm"], metrics["flow_rechazo_lpm"],
                    metrics["tds_in_raw"], metrics["tds_out_raw"],
                ),
            )
        else:
            trends = None


        # 4. Diagnóstico (se corre siempre, incluso sin métricas, para detectar faults)
        diag = diagnostic_engine.run(
            device_id, process_data, metrics, state, inputs, trends
        )


        # 5. Persistir diagnóstico solo si cambió o es relevante
        if diag.severity != "OK":
            db.execute(
                "INSERT INTO diagnostics (time, device_id, severity, code, message, details) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (timestamp, device_id, diag.severity, diag.code, diag.message,
                 json.dumps(diag.details)),
            )
            log.info(f"[{device_id}] Diagnóstico: {diag}")


        # 6. Actualizar estado en device_status
        db.execute(
            """
            INSERT INTO device_status
              (device_id, last_seen, online, state, last_severity, last_diag_code, last_diag_message,
               flow_perm_lpm, pressure_membrane,
               recovery, efficiency)
            VALUES (%s, %s, TRUE, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id) DO UPDATE SET
              last_seen         = EXCLUDED.last_seen,
              online            = TRUE,
              state             = EXCLUDED.state,
              last_severity     = EXCLUDED.last_severity,
              last_diag_code    = EXCLUDED.last_diag_code,
              last_diag_message = EXCLUDED.last_diag_message,
              flow_perm_lpm     = EXCLUDED.flow_perm_lpm,
              pressure_membrane = EXCLUDED.pressure_membrane,
              recovery          = EXCLUDED.recovery,
              efficiency        = EXCLUDED.efficiency
            """,
            (
                device_id, timestamp, state,
                diag.severity, diag.code, diag.message,
                metrics["flow_perm_lpm"] if metrics else None,
                validate_float(process_data.get("pressure_membrane_bar"), 0, 50),
                metrics["recovery"] if metrics else None,
                metrics["efficiency"] if metrics else None,
            ),
        )


        # 7. Alerta Telegram
        alert_manager.process(device_id, diag)




processor = MessageProcessor()


# ============================================================
# MQTT CALLBACKS
# ============================================================


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(MQTT_TOPIC)
        log.info(f"✅ MQTT conectado → suscrito a {MQTT_TOPIC}")
    else:
        log.error(f"❌ MQTT error de conexión (rc={rc})")




def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning(f"⚡ MQTT desconectado inesperadamente (rc={rc}). Reconectando...")




def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        processor.dispatch(msg.topic, payload)
    except Exception as e:
        log.error(f"Error procesando mensaje de {msg.topic}: {e}", exc_info=True)




# ============================================================
# MAIN
# ============================================================


def main():
    log.info("🚀 Fyntek RO Backend iniciando...")


    # Esperar que la DB levante (útil en docker-compose)
    for attempt in range(10):
        try:
            db.connect()
            break
        except Exception as e:
            log.warning(f"DB no disponible (intento {attempt+1}/10): {e}")
            time.sleep(3)
    else:
        log.critical("No se pudo conectar a la base de datos. Abortando.")
        return


    # Configurar MQTT
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message


    # Reconexión automática
    client.reconnect_delay_set(min_delay=1, max_delay=30)


    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        log.critical(f"No se pudo conectar al broker MQTT: {e}")
        return


    log.info("✅ Escuchando mensajes MQTT...")
    client.loop_forever()




if __name__ == "__main__":
    main()



