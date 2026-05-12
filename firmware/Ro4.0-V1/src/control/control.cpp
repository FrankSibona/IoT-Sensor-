#include "control.h"
#include <config.h>

// ================= SETUP =================
void Control::begin() {
    pinMode(PIN_R1, OUTPUT);
    pinMode(PIN_R2, OUTPUT);
    pinMode(PIN_R5, OUTPUT);
    pinMode(PIN_R6, OUTPUT);

    stopAll();
}

// ================= GETTERS =================

bool Control::isRunning() {
    return state == PRODUCING || state == STARTING || state == FLUSHING;
}

SystemState Control::getState() {
    return state;
}

int Control::getRetryCount() {
    return retryCount;
}

const char* Control::getStateName() {
    return stateToString(state);
}

// 👉 ESTO ES CLAVE PARA MQTT
OutputsState Control::getOutputs() {
    return outputs;
}

// ================= LOG =================

const char* Control::stateToString(SystemState s) {
    switch(s) {
        case IDLE: return "IDLE";
        case STARTING: return "STARTING";
        case PRODUCING: return "PRODUCING";
        case FLUSHING: return "FLUSHING";
        case STOPPING: return "STOPPING";
        case FAULT: return "FAULT";
        default: return "UNKNOWN";
    }
}

void Control::logStateChange(SystemState from, SystemState to) {
    Serial.print("[STATE] ");
    Serial.print(stateToString(from));
    Serial.print(" -> ");
    Serial.println(stateToString(to));
}

// ================= OUTPUTS =================

void Control::setOutputs(bool pumpLow, bool pumpHigh, bool flush, bool inlet) {

    outputs.pumpLow = pumpLow;
    outputs.pumpHigh = pumpHigh;
    outputs.valveFlush = flush;
    outputs.valveInlet = inlet;

    // no usados aún
    outputs.pumpInlet = false;
    outputs.pumpDose = false;

    digitalWrite(PIN_R1, pumpLow);
    digitalWrite(PIN_R2, pumpHigh);
    digitalWrite(PIN_R5, flush);
    digitalWrite(PIN_R6, inlet);
}

void Control::startLow() {
    setOutputs(true, false, false, true);
}

void Control::startHigh() {
    setOutputs(true, true, false, true);
}

void Control::flushOn() {
    setOutputs(true, false, true, true);
}

void Control::stopAll() {
    setOutputs(false, false, false, false);
}

// ================= FSM =================

void Control::update(Sensors &s, Commands &cmds) {

    // ===== COMMAND ENGINE =====
    // Remote commands are processed first so they take effect this iteration.
    // Order: applyCommand() → setPendingAck(EXECUTED) → clearPending().
    // EXECUTED means the FSM transition was applied, not that hardware confirmed it.
    // If applyCommand() ever gains a return value or internal validation,
    // condition setPendingAck(EXECUTED) on its result here — no structural change needed.
    if (cmds.hasPending()) {
        if (cmds.hasExpired()) {
            cmds.clearPending();
        } else {
            const PendingCmd cmd = cmds.getPending();  // value copy before slot is cleared
            if (isValidTransition(cmd.type)) {
                applyCommand(cmd.type);
                cmds.setPendingAck(cmd.id, cmd.type, AckStatus::EXECUTED, "");
                cmds.clearPending();
            } else {
                cmds.setPendingAck(cmd.id, cmd.type, AckStatus::REJECTED, "invalid_fsm_state");
                cmds.clearPending();
            }
        }
    }

    // ===== Persistencia =====
    if (s.demanda()) {
        if (demandaStart == 0) demandaStart = millis();
    } else demandaStart = 0;

    if (s.crudoDisponible()) {
        if (crudoStart == 0) crudoStart = millis();
    } else crudoStart = 0;

    if (s.presionOK()) {
        if (presionStart == 0) presionStart = millis();
    } else presionStart = 0;

    bool demandaOK = demandaStart && (millis() - demandaStart > 2000);
    bool crudoOK   = crudoStart   && (millis() - crudoStart > 2000);
    bool presionOK = presionStart && (millis() - presionStart > 2000);

    if (state != lastState) {
        logStateChange(lastState, state);
        lastState = state;
    }

    switch(state) {

        case IDLE:
            stopAll();

            if (demandaOK && crudoOK) {
                Serial.println("[EVENT] Demanda detectada -> arranque");
                state = STARTING;
                stateStartTime = millis();
            }
            break;

        case STARTING:
            startLow();

            if (!crudoOK) {
                Serial.println("[FAULT] Sin agua de crudo");
                state = IDLE;
                break;
            }

            if (millis() - stateStartTime > LOW_PUMP_FILL_TIME) {
                startHigh();
            }

            if (millis() - stateStartTime > PRESSURE_CHECK_TIME) {

                if (presionOK) {
                    Serial.println("[EVENT] Presión OK");
                    retryCount = 0;
                    state = PRODUCING;
                } else {
                    retryCount++;
                    Serial.println("[FAULT] Presión no alcanzada");
                    state = IDLE;
                    retryTimer = millis();
                }
            }
            break;

        case PRODUCING:
            startHigh();

            if (!demandaOK) {
                Serial.println("[EVENT] Fin demanda -> flushing");
                state = FLUSHING;
                stateStartTime = millis();
            }

            if (!crudoOK || !presionOK) {
                Serial.println("[FAULT] Pérdida condición");
                state = IDLE;
            }
            break;

        case FLUSHING:
            flushOn();

            if (millis() - stateStartTime > FLUSH_TDS_TIME) {
                Serial.println("[EVENT] Fin flushing");
                state = IDLE;
            }
            break;

        case FAULT:
            stopAll();
            break;

        case STOPPING:
            state = IDLE;
            break;
    }

    if (retryCount >= MAX_RETRIES) {
        state = FAULT;
    }

    if (retryCount > 0 && millis() - retryTimer < RETRY_DELAY) {
        return;
    }
}

// ================= COMMAND VALIDATION =================

bool Control::isValidTransition(CommandType cmd) const {
    switch (cmd) {
        case CommandType::START:
            return state == IDLE;
        case CommandType::STOP:
            return state == STARTING || state == PRODUCING || state == FLUSHING;
        case CommandType::FLUSH:
            return state == PRODUCING;
        case CommandType::RST:
            // Only valid in fault/error states — rejected in IDLE to prevent
            // silent no-ops becoming unsafe if reset behavior gains side-effects.
            return state == FAULT || state == STARTING || state == FLUSHING;
        default:
            return false;
    }
}

// ================= COMMAND APPLICATION =================

void Control::applyCommand(CommandType cmd) {
    switch (cmd) {
        case CommandType::START:
            state = STARTING;
            stateStartTime = millis();
            break;
        case CommandType::STOP:
            // From PRODUCING: flush membrane before halting.
            // From STARTING / FLUSHING: abort immediately.
            state = (state == PRODUCING) ? FLUSHING : IDLE;
            stateStartTime = millis();
            break;
        case CommandType::FLUSH:
            // Only reachable from PRODUCING (enforced by isValidTransition).
            state = FLUSHING;
            stateStartTime = millis();
            break;
        case CommandType::RST:
            retryCount = 0;
            state = IDLE;
            break;
        default:
            break;
    }
}