#include <Arduino.h>
#include "sensors/sensors.h"
#include "control/control.h"
#include "comms/comms.h"

Sensors sensors;
Control control;
Comms comms;

void setup() {
    Serial.begin(115200);

    sensors.begin();
    control.begin();
    comms.begin();

    Serial.println("=== SYSTEM START ===");
}

void loop() {

    // 1. Sensores
    sensors.update();

    // 2. Control (CRÍTICO)
    control.update(sensors);

    // 3. Comunicaciones (NO BLOQUEANTE)
    comms.update(sensors, control);

    // 4. Debug liviano
    static unsigned long lastDebug = 0;
    if (millis() - lastDebug > 1000) {
        lastDebug = millis();

        Serial.print("[DEBUG] RUNNING: ");
        Serial.print(control.isRunning());
        Serial.print(" | STATE: ");
        Serial.println(control.getStateName());
    }

    // 5. Pequeño respiro al CPU (IMPORTANTE)
    delay(10);
}