- ============================================================
-- Fyntek RO Backend - Schema v3.3
-- Compatible con worker v3.3
-- PostgreSQL >= 13
-- ============================================================
-- Cambios vs v3.2:
--   device_config:  +daily_target_liters
--   device_status:  +campos de negocio (waste, fulfillment, risk, degradation)
--   business_metrics: tabla nueva con KPIs de negocio por día
-- ============================================================

CREATE TABLE IF NOT EXISTS telemetry_process (
    time                    TIMESTAMPTZ     NOT NULL,
    device_id               TEXT            NOT NULL,
    flow_perm_lpm           FLOAT,
    flow_rechazo_lpm        FLOAT,
    pressure_membrane_bar   FLOAT,
    pressure_brine_bar      FLOAT,
    volume_perm_l           FLOAT,
    volume_rechazo_l        FLOAT,
    fw_version              TEXT
);
CREATE INDEX IF NOT EXISTS idx_process_device_time
    ON telemetry_process (device_id, time DESC);

CREATE TABLE IF NOT EXISTS telemetry_quality (
    time            TIMESTAMPTZ     NOT NULL,
    device_id       TEXT            NOT NULL,
    tds_in_raw      FLOAT,
    tds_out_raw     FLOAT,
    fw_version      TEXT
);
CREATE INDEX IF NOT EXISTS idx_quality_device_time
    ON telemetry_quality (device_id, time DESC);

CREATE TABLE IF NOT EXISTS telemetry_state (
    time            TIMESTAMPTZ     NOT NULL,
    device_id       TEXT            NOT NULL,
    state           TEXT            NOT NULL,
    state_numeric   SMALLINT        NOT NULL,
    running         BOOLEAN,
    retry_count     SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_state_device_time
    ON telemetry_state (device_id, time DESC);

CREATE TABLE IF NOT EXISTS telemetry_inputs (
    time        TIMESTAMPTZ     NOT NULL,
    device_id   TEXT            NOT NULL,
    demand      BOOLEAN,
    crudo_ok    BOOLEAN,
    dose_ok     BOOLEAN,
    presostato  BOOLEAN,
    reserva1    BOOLEAN,
    reserva2    BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_inputs_device_time
    ON telemetry_inputs (device_id, time DESC);

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

CREATE TABLE IF NOT EXISTS metrics (
    time                TIMESTAMPTZ     NOT NULL,
    device_id           TEXT            NOT NULL,
    recovery            FLOAT,
    efficiency          FLOAT,
    rejection_ratio     FLOAT,
    delta_pressure_bar  FLOAT,
    flow_perm_lpm       FLOAT,
    flow_rechazo_lpm    FLOAT,
    tds_in_raw          FLOAT,
    tds_out_raw         FLOAT,
    cost_per_liter      FLOAT
);
CREATE INDEX IF NOT EXISTS idx_metrics_device_time
    ON metrics (device_id, time DESC);

CREATE TABLE IF NOT EXISTS diagnostics (
    time        TIMESTAMPTZ     NOT NULL,
    device_id   TEXT            NOT NULL,
    severity    TEXT            NOT NULL,
    code        TEXT            NOT NULL,
    message     TEXT            NOT NULL,
    action      TEXT,
    details     JSONB
);
CREATE INDEX IF NOT EXISTS idx_diagnostics_device_time
    ON diagnostics (device_id, time DESC);

-- ============================================================
-- TABLA: device_status
-- state       → qué está haciendo el equipo (operativo, cambia rápido)
-- health_*    → cómo está el sistema (persiste, independiente del state)
-- biz_*       → métricas de negocio en tiempo real
-- ============================================================
CREATE TABLE IF NOT EXISTS device_status (
    device_id               TEXT        PRIMARY KEY,
    last_seen               TIMESTAMPTZ,
    online                  BOOLEAN     DEFAULT FALSE,

    -- Estado operativo
    state                   TEXT,

    -- Último diagnóstico del ciclo actual
    last_severity           TEXT,
    last_diag_code          TEXT,
    last_diag_message       TEXT,
    last_action             TEXT,

    -- Lecturas de proceso
    flow_perm_lpm           FLOAT,
    pressure_membrane       FLOAT,
    recovery                FLOAT,
    efficiency              FLOAT,

    -- Salud persistente (no se resetea al apagarse)
    health_status           TEXT        DEFAULT 'UNKNOWN',
    health_code             TEXT        DEFAULT 'UNKNOWN',
    health_message          TEXT,
    health_action           TEXT,
    health_updated_at       TIMESTAMPTZ,

    -- ── MÉTRICAS DE NEGOCIO (nuevo en v3.3) ──────────────────
    -- Producción
    biz_liters_today        FLOAT,      -- litros producidos hoy
    biz_target_liters       FLOAT,      -- objetivo del día (de device_config)
    biz_fulfillment_pct     FLOAT,      -- cumplimiento vs objetivo [0-100]
    -- Desperdicio
    biz_waste_liters_today  FLOAT,      -- litros rechazados hoy
    biz_waste_pct           FLOAT,      -- % rechazo sobre total [0-100]
    -- Riesgo operativo
    biz_risk_level          TEXT,       -- LOW | MEDIUM | HIGH | CRITICAL
    biz_risk_score          FLOAT,      -- score numérico [0-100]
    -- Degradación
    biz_degradation_pct     FLOAT,      -- % pérdida eficiencia vs baseline (negativo = degradó)
    biz_degradation_days    INT,        -- en cuántos días ocurrió
    biz_degradation_label   TEXT,       -- ej: "Perdió 8.3% en 7 días"
    -- Frescura del diagnóstico de salud
    biz_health_age_hours    FLOAT       -- horas desde el último update de salud
);

CREATE TABLE IF NOT EXISTS devices (
    device_id           TEXT        PRIMARY KEY,
    friendly_name       TEXT,
    telegram_chat_id    TEXT,
    registered_at       TIMESTAMPTZ DEFAULT NOW(),
    fw_version          TEXT,
    notes               TEXT
);

-- ============================================================
-- TABLA: device_config
-- +daily_target_liters: objetivo de producción diaria (nuevo v3.3)
-- ============================================================
CREATE TABLE IF NOT EXISTS device_config (
    device_id               TEXT        PRIMARY KEY,
    pump_power_kw           FLOAT       DEFAULT 0.75,
    cost_kwh                FLOAT       DEFAULT 0.12,
    cost_water_m3           FLOAT       DEFAULT 0.80,
    target_recovery         FLOAT       DEFAULT 0.65,
    target_efficiency       FLOAT       DEFAULT 0.92,
    daily_target_liters     FLOAT       DEFAULT 0,       -- ← nuevo v3.3 (0 = sin objetivo)
    friendly_name           TEXT,
    location                TEXT,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS learning_sessions (
    id              SERIAL          PRIMARY KEY,
    device_id       TEXT            NOT NULL,
    started_at      TIMESTAMPTZ     NOT NULL,
    finished_at     TIMESTAMPTZ,
    duration_min    INT             DEFAULT 30,
    status          TEXT            DEFAULT 'RUNNING',
    samples         INT             DEFAULT 0
);

-- ============================================================
-- TABLA: device_baseline
-- ============================================================
CREATE TABLE IF NOT EXISTS device_baseline (
    device_id               TEXT    PRIMARY KEY,
    learned_at              TIMESTAMPTZ,
    session_id              INT,
    efficiency_mean         FLOAT,
    efficiency_std          FLOAT,
    recovery_mean           FLOAT,
    recovery_std            FLOAT,
    flow_perm_mean          FLOAT,
    flow_perm_std           FLOAT,
    delta_pressure_mean     FLOAT,
    delta_pressure_std      FLOAT,

    efficiency_warn_low_learned     FLOAT,
    efficiency_warn_low_manual      FLOAT,
    efficiency_warn_low_source      TEXT    DEFAULT 'learned',
    efficiency_crit_low_learned     FLOAT,
    efficiency_crit_low_manual      FLOAT,
    efficiency_crit_low_source      TEXT    DEFAULT 'learned',
    recovery_warn_low_learned       FLOAT,
    recovery_warn_low_manual        FLOAT,
    recovery_warn_low_source        TEXT    DEFAULT 'learned',
    recovery_warn_high_learned      FLOAT,
    recovery_warn_high_manual       FLOAT,
    recovery_warn_high_source       TEXT    DEFAULT 'learned',
    flow_perm_warn_low_learned      FLOAT,
    flow_perm_warn_low_manual       FLOAT,
    flow_perm_warn_low_source       TEXT    DEFAULT 'learned',
    pressure_warn_high_learned      FLOAT,
    pressure_warn_high_manual       FLOAT,
    pressure_warn_high_source       TEXT    DEFAULT 'learned',
    pressure_crit_high_learned      FLOAT,
    pressure_crit_high_manual       FLOAT,
    pressure_crit_high_source       TEXT    DEFAULT 'learned',
    delta_pressure_warn_high_learned    FLOAT,
    delta_pressure_warn_high_manual     FLOAT,
    delta_pressure_warn_high_source     TEXT    DEFAULT 'learned'
);

-- ============================================================
-- TABLA: business_metrics (nuevo v3.3)
-- KPIs de negocio calculados una vez por día por dispositivo.
-- Histórico completo para trending de largo plazo.
-- ============================================================
CREATE TABLE IF NOT EXISTS business_metrics (
    day                     DATE        NOT NULL,
    device_id               TEXT        NOT NULL,
    PRIMARY KEY (day, device_id),

    -- Producción
    liters_produced         FLOAT,      -- litros de permeado producidos
    liters_rejected         FLOAT,      -- litros de rechazo
    daily_target_liters     FLOAT,      -- objetivo del día (snapshot de config)
    fulfillment_pct         FLOAT,      -- % cumplimiento vs objetivo
    -- Desperdicio
    waste_pct               FLOAT,      -- rechazo / (producido + rechazo) * 100
    -- Eficiencia
    avg_efficiency          FLOAT,      -- eficiencia media del día [0-1]
    avg_recovery            FLOAT,      -- recovery medio del día [0-1]
    -- Operación
    hours_producing         FLOAT,      -- horas en estado PRODUCING
    cycle_count             INT,        -- ciclos ON/OFF
    -- Costo
    estimated_cost          FLOAT,      -- costo estimado del día
    -- Riesgo
    risk_level              TEXT,       -- riesgo dominante del día
    -- Metadatos
    calculated_at           TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_biz_device_day
    ON business_metrics (device_id, day DESC);

-- ============================================================
-- VISTA: daily_production
-- ============================================================
CREATE OR REPLACE VIEW daily_production AS
SELECT
    device_id,
    DATE(time AT TIME ZONE 'America/Argentina/Buenos_Aires') AS day,
    MAX(volume_perm_l)    - MIN(volume_perm_l)               AS liters_produced,
    MAX(volume_rechazo_l) - MIN(volume_rechazo_l)            AS liters_rejected,
    ROUND(CAST(
        (MAX(volume_perm_l) - MIN(volume_perm_l)) /
        NULLIF(
            (MAX(volume_perm_l)    - MIN(volume_perm_l)) +
            (MAX(volume_rechazo_l) - MIN(volume_rechazo_l)),
        0) * 100
    AS numeric), 1) AS recovery_pct
FROM telemetry_process
WHERE volume_perm_l IS NOT NULL
GROUP BY device_id, DATE(time AT TIME ZONE 'America/Argentina/Buenos_Aires')
ORDER BY day DESC;

-- ============================================================
-- TABLA: device_commands
-- Command Engine v3 — lifecycle: SENT → RECEIVED → ACCEPTED
--                                     → REJECTED | EXECUTED | TIMEOUT
-- ============================================================
CREATE TABLE IF NOT EXISTS device_commands (
    id            SERIAL       PRIMARY KEY,
    command_id    TEXT         NOT NULL UNIQUE,
    device_id     TEXT         NOT NULL,
    cmd           TEXT         NOT NULL
                  CHECK (cmd IN ('START','STOP','FLUSH','RST')),
    status        TEXT         NOT NULL DEFAULT 'SENT'
                  CHECK (status IN ('SENT','RECEIVED','ACCEPTED',
                                    'REJECTED','EXECUTED','TIMEOUT')),
    issued_by     TEXT         DEFAULT 'api',
    retry_count   INTEGER      NOT NULL DEFAULT 0,
    issued_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    deadline_at   TIMESTAMPTZ,
    received_at   TIMESTAMPTZ,
    accepted_at   TIMESTAMPTZ,
    rejected_at   TIMESTAMPTZ,
    executed_at   TIMESTAMPTZ,
    timeout_at    TIMESTAMPTZ,
    last_ack_at   TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    reject_reason TEXT,
    details       JSONB        DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_commands_device_time
    ON device_commands (device_id, issued_at DESC);
CREATE INDEX IF NOT EXISTS idx_commands_id
    ON device_commands (command_id);
CREATE INDEX IF NOT EXISTS idx_commands_active
    ON device_commands (device_id, status)
    WHERE status IN ('SENT','RECEIVED','ACCEPTED');
CREATE UNIQUE INDEX IF NOT EXISTS uq_commands_one_active_per_device
    ON device_commands (device_id)
    WHERE status IN ('SENT','RECEIVED','ACCEPTED');

-- ============================================================
-- MIGRACIÓN DESDE v3.2 (si ya tenés la DB):
-- ============================================================
-- ALTER TABLE device_config  ADD COLUMN IF NOT EXISTS daily_target_liters FLOAT DEFAULT 0;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_liters_today      FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_target_liters     FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_fulfillment_pct   FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_waste_liters_today FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_waste_pct         FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_risk_level        TEXT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_risk_score        FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_degradation_pct   FLOAT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_degradation_days  INT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_degradation_label TEXT;
-- ALTER TABLE device_status  ADD COLUMN IF NOT EXISTS biz_health_age_hours  FLOAT;
-- CREATE TABLE IF NOT EXISTS business_metrics ( ... ver definición arriba ... );