#pragma once

#include <Arduino.h>
#include "sensors/sensors.h"

// ===================== ESTADOS =====================
enum SystemState {
    IDLE,
    STARTING,
    PRODUCING,
    FLUSHING,
    STOPPING,
    FAULT
};

// ===================== OUTPUTS =====================
struct OutputsState {
    bool pumpLow;
    bool pumpHigh;
    bool pumpInlet;
    bool pumpDose;
    bool valveFlush;
    bool valveInlet;
};

// ===================== CONTROL =====================
class Control {
public:
    void begin();
    void update(Sensors &s);

    bool isRunning();
    SystemState getState();
    const char* getStateName();
    int getRetryCount();

    OutputsState getOutputs();

private:
    SystemState state = IDLE;
    SystemState lastState = IDLE;

    unsigned long stateStartTime = 0;
    unsigned long retryTimer = 0;
    int retryCount = 0;

    unsigned long demandaStart = 0;
    unsigned long crudoStart = 0;
    unsigned long presionStart = 0;

    // 🔥 NUEVO: estado REAL de salidas
    OutputsState outputs;

    // OUTPUTS
    void setOutputs(bool pumpLow, bool pumpHigh, bool flush, bool inlet);
    void startLow();
    void startHigh();
    void flushOn();
    void stopAll();

    void logStateChange(SystemState from, SystemState to);
    const char* stateToString(SystemState s);
};