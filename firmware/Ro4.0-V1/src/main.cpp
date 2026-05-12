#include <Arduino.h>
#include "sensors/sensors.h"
#include "control/control.h"
#include "comms/comms.h"
#include "commands/commands.h"

Sensors  sensors;
Control  control;
Comms    comms;
Commands commands;

void setup() {
    Serial.begin(115200);

    sensors.begin();
    control.begin();
    commands.begin();
    comms.begin(commands);

    Serial.println("=== SYSTEM START ===");
}

void loop() {

    // 1. Sensores
    sensors.update();

    // 2. Control + Command Engine (CRÍTICO)
    control.update(sensors, commands);

    // 3. Comunicaciones — procesa callbacks MQTT y publica ACK pendiente
    comms.update(sensors, control, commands);

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