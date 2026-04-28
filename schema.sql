-- ============================================================
-- Fyntek RO Backend - Schema v2.0
-- Compatible con PostgreSQL >= 13 y TimescaleDB (opcional)
-- ============================================================


-- EXTENSIÓN OPCIONAL: habilitar TimescaleDB para mejor
-- rendimiento en series de tiempo a escala.
-- Si no lo usás, comentá estas líneas.
-- CREATE EXTENSION IF NOT EXISTS timescaledb;


-- ============================================================
-- TABLA: telemetry_process
-- Fuente: fyntek/{device_id}/process (cada 1 segundo)
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_process (
    time                    TIMESTAMPTZ     NOT NULL,
    device_id               TEXT            NOT NULL,
    flow_perm_lpm           FLOAT,          -- Caudal permeado (L/min)
    flow_rechazo_lpm        FLOAT,          -- Caudal rechazo (L/min)
    pressure_membrane_bar   FLOAT,          -- Presión en membrana (bar)
    pressure_brine_bar      FLOAT,          -- Presión en salida brine/rechazo (bar)
    volume_perm_l           FLOAT,          -- Volumen acumulado permeado (L, desde NVS)
    volume_rechazo_l        FLOAT,          -- Volumen acumulado rechazo (L, desde NVS)
    fw_version              TEXT
);
CREATE INDEX IF NOT EXISTS idx_process_device_time
    ON telemetry_process (device_id, time DESC);


-- Si usás TimescaleDB:
-- SELECT create_hypertable('telemetry_process', 'time', if_not_exists => TRUE);


-- ============================================================
-- TABLA: telemetry_quality
-- Fuente: fyntek/{device_id}/quality (cada 10 segundos)
-- NOTA: El firmware envía voltajes (0-5V) como tds_in_ppm y
--       tds_out_ppm. La conversión a PPM real requiere la curva
--       de calibración del sensor TDS específico.
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_quality (
    time            TIMESTAMPTZ     NOT NULL,
    device_id       TEXT            NOT NULL,
    tds_in_raw      FLOAT,          -- Voltaje raw (0-5V) del sensor TDS entrada
    tds_out_raw     FLOAT,          -- Voltaje raw (0-5V) del sensor TDS salida
    fw_version      TEXT
);
CREATE INDEX IF NOT EXISTS idx_quality_device_time
    ON telemetry_quality (device_id, time DESC);


-- ============================================================
-- TABLA: telemetry_state
-- Fuente: fyntek/{device_id}/state (on-change)
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_state (
    time            TIMESTAMPTZ     NOT NULL,
    device_id       TEXT            NOT NULL,
    state           TEXT            NOT NULL,   -- IDLE, STARTING, PRODUCING, FLUSHING, STOPPING, FAULT
    state_numeric   SMALLINT        NOT NULL,   -- 0=IDLE, 1=STARTING, 2=PRODUCING, 3=FLUSHING, 4=STOPPING, 5=FAULT
    running         BOOLEAN,
    retry_count     SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_state_device_time
    ON telemetry_state (device_id, time DESC);


-- ============================================================
-- TABLA: telemetry_inputs
-- Fuente: fyntek/{device_id}/inputs (on-change)
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_inputs (
    time        TIMESTAMPTZ     NOT NULL,
    device_id   TEXT            NOT NULL,
    demand      BOOLEAN,    -- D1: señal de demanda
    crudo_ok    BOOLEAN,    -- D2: agua cruda disponible
    dose_ok     BOOLEAN,    -- D3: dosificación OK
    presostato  BOOLEAN,    -- D4: presostato OK
    reserva1    BOOLEAN,    -- D5: reserva 1
    reserva2    BOOLEAN     -- D6: reserva 2
);
CREATE INDEX IF NOT EXISTS idx_inputs_device_time
    ON telemetry_inputs (device_id, time DESC);


-- ============================================================
-- TABLA: telemetry_outputs
-- Fuente: fyntek/{device_id}/outputs (on-change)
-- ============================================================
CREATE TABLE IF NOT EXISTS telemetry_outputs (
    time            TIMESTAMPTZ     NOT NULL,
    device_id       TEXT            NOT NULL,
    pump_low        BOOLEAN,
    pump_high       BOOLEAN,
    pump_inlet      BOOLEAN,
    pump_dose       BOOLEAN,
    valve_flush     BOOLEAN,
    valve_inlet     BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_outputs_device_time
    ON telemetry_outputs (device_id, time DESC);


-- ============================================================
-- TABLA: metrics
-- Generada por el backend (KPI Engine)
-- Se calcula cada vez que llegan datos de process + quality
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics (
    time                TIMESTAMPTZ     NOT NULL,
    device_id           TEXT            NOT NULL,
    -- Core KPIs
    recovery            FLOAT,      -- flow_perm / (flow_perm + flow_rechazo), [0-1]
    efficiency          FLOAT,      -- 1 - (tds_out / tds_in), [0-1], calidad membrana
    rejection_ratio     FLOAT,      -- flow_rechazo / flow_perm
    delta_pressure_bar  FLOAT,      -- pressure_membrane - pressure_brine
    -- Caudales
    flow_perm_lpm       FLOAT,
    flow_rechazo_lpm    FLOAT,
    -- Calidad
    tds_in_raw          FLOAT,
    tds_out_raw         FLOAT
);
CREATE INDEX IF NOT EXISTS idx_metrics_device_time
    ON metrics (device_id, time DESC);


-- ============================================================
-- TABLA: diagnostics
-- Generada por el backend (Diagnostic Engine)
-- Solo se inserta cuando hay un evento diagnóstico nuevo.
-- ============================================================
CREATE TABLE IF NOT EXISTS diagnostics (
    time        TIMESTAMPTZ     NOT NULL,
    device_id   TEXT            NOT NULL,
    severity    TEXT            NOT NULL,   -- OK, WARNING, CRITICAL
    code        TEXT            NOT NULL,   -- ej: MEMBRANE_FOULING
    message     TEXT            NOT NULL,   -- texto legible
    details     JSONB                       -- variables que dispararon el diagnóstico
);
CREATE INDEX IF NOT EXISTS idx_diagnostics_device_time
    ON diagnostics (device_id, time DESC);


-- ============================================================
-- TABLA: device_status
-- Estado en tiempo real de cada dispositivo.
-- Se hace UPSERT en cada mensaje.
-- ============================================================
CREATE TABLE IF NOT EXISTS device_status (
    device_id           TEXT        PRIMARY KEY,
    last_seen           TIMESTAMPTZ,
    online              BOOLEAN     DEFAULT FALSE,
    state               TEXT,
    -- Último diagnóstico activo
    last_severity       TEXT,
    last_diag_code      TEXT,
    last_diag_message   TEXT,
    -- Última lectura de proceso
    flow_perm_lpm       FLOAT,
    pressure_membrane   FLOAT,
    -- Última métrica
    recovery            FLOAT,
    efficiency          FLOAT
);


CREATE TABLE IF NOT EXISTS devices (
    device_id       TEXT        PRIMARY KEY,
    -- Estos dos campos son los que editás manualmente (o via script)
    friendly_name   TEXT,               -- ej: "Osmosis Planta Norte"
    telegram_chat_id TEXT,              -- chat_id del cliente
    -- El resto se llena automático
    registered_at   TIMESTAMPTZ DEFAULT NOW(),
    fw_version      TEXT,
    notes           TEXT                -- campo libre para anotaciones
);





