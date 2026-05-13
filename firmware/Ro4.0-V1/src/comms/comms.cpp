#include "comms.h"
#include <WiFi.h>
#include <WiFiManager.h>
#include <PubSubClient.h>
#include <time.h>
#include <string.h>

#include "../sensors/sensors.h"
#include "../control/control.h"
#include "../commands/commands.h"

// ================= DEVICE ID =================
// Must be declared before mqttCallback — static free functions only see
// names declared above their definition in the same translation unit.

String device_id;

// ================= COMMAND CALLBACK =================

static Commands* s_cmds = nullptr;

// Maps ReceiveResult to a human-readable string for field logs.
// ACCEPTED(0) DUPLICATE(1) BUSY(2) INVALID_JSON(3) UNKNOWN_COMMAND(4)
static const char* receiveResultName(ReceiveResult r) {
    switch (r) {
        case ReceiveResult::ACCEPTED:        return "ACCEPTED";
        case ReceiveResult::DUPLICATE:       return "DUPLICATE";
        case ReceiveResult::BUSY:            return "BUSY";
        case ReceiveResult::INVALID_JSON:    return "INVALID_JSON";
        case ReceiveResult::UNKNOWN_COMMAND: return "UNKNOWN_COMMAND";
        default:                             return "UNKNOWN";
    }
}

// Free function required by PubSubClient. s_cmds is set in begin().
// Compares the full topic against the exact expected pattern
// "fyntek/{device_id}/cmd" — prevents false positives from suffixes
// like /cmd/ack or malformed topics like /xcmd.
// Does NOT publish — ACK is deferred to comms.update() after the
// FSM decides in the next control.update() call.
static void mqttCallback(char* topic, byte* payload, unsigned int length) {
    if (!s_cmds) return;

    char expected[64];
    snprintf(expected, sizeof(expected), "fyntek/%s/cmd", device_id.c_str());
    if (strcmp(topic, expected) != 0) return;

    ReceiveResult result = s_cmds->receive(payload, length);
    if (result != ReceiveResult::ACCEPTED) {
        Serial.print("[CMD] recv rechazado: ");
        Serial.println(receiveResultName(result));
    }
}

// ================= CONFIG =================

const char* mqtt_server = "159.112.132.176";
const int mqtt_port = 1883;

const char* mqtt_user = "kairox";
const char* mqtt_pass = "admin0102";

const char* fw_version = "1.1.0";

// NTP
const char* ntpServer = "pool.ntp.org";
const long gmtOffset_sec = -3 * 3600;
const int daylightOffset_sec = 0;

// ================= OBJETOS =================

WiFiManager wm;
WiFiClient espClient;
PubSubClient mqttClient(espClient);

// ================= TIMERS =================

unsigned long lastWifiCheck = 0;
unsigned long lastMqttReconnect = 0;

unsigned long lastProcess = 0;
unsigned long lastQuality = 0;
unsigned long lastHeartbeat = 0;

String getDeviceID() {
    uint64_t mac = ESP.getEfuseMac();

    char id[20];
    sprintf(id, "ESP32_%04X%08X",
        (uint16_t)(mac >> 32),
        (uint32_t)mac);

    return String(id);
}

// ================= HELPERS =================

long getTimestamp() {
    struct tm timeinfo;
    if (!getLocalTime(&timeinfo)) return 0;
    return mktime(&timeinfo);
}

String baseTopic(String sub) {
    return "fyntek/" + device_id + "/" + sub;
}

// ================= WIFI =================

void setupWiFi() {
    WiFi.mode(WIFI_STA);

    if (!wm.autoConnect("FYNTEK_SETUP")) {
        Serial.println("❌ No WiFi → restart");
        delay(3000);
        ESP.restart();
    }

    Serial.println("✅ WiFi conectado");

    configTime(gmtOffset_sec, daylightOffset_sec, ntpServer);
}

// ================= MQTT =================

void Comms::reconnect() {

    if (mqttClient.connected()) return;

    unsigned long now = millis();
    if (now - lastMqttReconnect < 5000) return;

    lastMqttReconnect = now;

    String clientId = "ESP32-" + device_id;

    if (mqttClient.connect(clientId.c_str(), mqtt_user, mqtt_pass)) {

        Serial.println("✅ MQTT conectado");

        mqttClient.subscribe(baseTopic("cmd").c_str());

        // 🔥 SNAPSHOT REAL
        sendSnapshot = true;

    } else {
        Serial.println("❌ MQTT reconectando...");
    }
}

// ================= INIT =================

void Comms::begin(Commands &cmds) {

    Serial.println("[COMMS] Init");

    device_id = getDeviceID();

    Serial.print("DEVICE ID: ");
    Serial.println(device_id);

    setupWiFi();

    s_cmds = &cmds;
    mqttClient.setServer(mqtt_server, mqtt_port);
    mqttClient.setCallback(mqttCallback);
}

// ================= UPDATE =================

void Comms::update(Sensors &s, Control &c, Commands &cmds) {

    unsigned long now = millis();

    // ===== WIFI =====
    if (now - lastWifiCheck > 10000) {
        lastWifiCheck = now;

        if (WiFi.status() != WL_CONNECTED) {
            Serial.println("[COMMS] WiFi reconectando...");
            WiFi.reconnect();
        }
    }

    // ===== MQTT =====
    reconnect();
    mqttClient.loop();

    if (!mqttClient.connected()) return;

    long ts = getTimestamp();

    // ================= SNAPSHOT REAL =================
    if (sendSnapshot) {

        sendSnapshot = false;

        Serial.println("📸 SNAPSHOT COMPLETO");

        // ===== PROCESS =====
        {
            String json = "{";
            json += "\"device_id\":\"" + device_id + "\",";
            json += "\"fw_version\":\"" + String(fw_version) + "\",";
            json += "\"ts\":" + String(ts) + ",";
            json += "\"flow_perm_lpm\":" + String(s.getFlow1()) + ",";
            json += "\"flow_rechazo_lpm\":" + String(s.getFlow2()) + ",";
            json += "\"pressure_membrane_bar\":" + String(s.getPressure1()) + ",";
            json += "\"pressure_brine_bar\":" + String(s.getPressure2()) + ",";
            json += "\"volume_perm_l\":" + String(s.getTotalPerm()) + ",";
            json += "\"volume_rechazo_l\":" + String(s.getTotalRech());
            json += "}";

            mqttClient.publish(baseTopic("process").c_str(), json.c_str());
        }

        // ===== QUALITY =====
        {
            String json = "{";
            json += "\"device_id\":\"" + device_id + "\",";
            json += "\"fw_version\":\"" + String(fw_version) + "\",";
            json += "\"ts\":" + String(ts) + ",";
            json += "\"tds_in_ppm\":" + String(s.getTDS1()) + ",";
            json += "\"tds_out_ppm\":" + String(s.getTDS2());
            json += "}";

            mqttClient.publish(baseTopic("quality").c_str(), json.c_str());
        }

        // ===== STATE =====
        {
            String json = "{";
            json += "\"device_id\":\"" + device_id + "\",";
            json += "\"ts\":" + String(ts) + ",";
            json += "\"state\":\"" + String(c.getStateName()) + "\",";
            json += "\"running\":" + String(c.isRunning()) + ",";
            json += "\"retry\":" + String(c.getRetryCount());
            json += "}";

            mqttClient.publish(baseTopic("state").c_str(), json.c_str());
        }

        // ===== OUTPUTS =====
        {
            OutputsState out = c.getOutputs();

            String json = "{";
            json += "\"device_id\":\"" + device_id + "\",";
            json += "\"ts\":" + String(ts) + ",";
            json += "\"pump_low\":" + String(out.pumpLow) + ",";
            json += "\"pump_high\":" + String(out.pumpHigh) + ",";
            json += "\"pump_inlet\":" + String(out.pumpInlet) + ",";
            json += "\"pump_dose\":" + String(out.pumpDose) + ",";
            json += "\"valve_flush\":" + String(out.valveFlush) + ",";
            json += "\"valve_inlet\":" + String(out.valveInlet);
            json += "}";

            mqttClient.publish(baseTopic("outputs").c_str(), json.c_str());
        }

        // ===== INPUTS =====
        {
            String json = "{";
            json += "\"device_id\":\"" + device_id + "\",";
            json += "\"ts\":" + String(ts) + ",";
            json += "\"demand\":" + String(s.getD1()) + ",";
            json += "\"crudo_ok\":" + String(s.getD2()) + ",";
            json += "\"dose_ok\":" + String(s.getD3()) + ",";
            json += "\"presostato\":" + String(s.getD4()) + ",";
            json += "\"reserva1\":" + String(s.getD5()) + ",";
            json += "\"reserva2\":" + String(s.getD6());
            json += "}";

            mqttClient.publish(baseTopic("inputs").c_str(), json.c_str());
        }
    }


    // ================= PROCESS =================
    if (now - lastProcess > 1000) {

        lastProcess = now;

        String json = "{";
        json += "\"device_id\":\"" + device_id + "\",";
        json += "\"fw_version\":\"" + String(fw_version) + "\",";
        json += "\"ts\":" + String(ts) + ",";

        json += "\"flow_perm_lpm\":" + String(s.getFlow1()) + ",";
        json += "\"flow_rechazo_lpm\":" + String(s.getFlow2()) + ",";
        json += "\"pressure_membrane_bar\":" + String(s.getPressure1()) + ",";
        json += "\"pressure_brine_bar\":" + String(s.getPressure2()) + ",";
        json += "\"volume_perm_l\":" + String(s.getTotalPerm()) + ",";
        json += "\"volume_rechazo_l\":" + String(s.getTotalRech());

        json += "}";

        mqttClient.publish(baseTopic("process").c_str(), json.c_str());
    }

    // ================= QUALITY =================
    if (now - lastQuality > 10000) {

        lastQuality = now;

        String json = "{";
        json += "\"device_id\":\"" + device_id + "\",";
        json += "\"fw_version\":\"" + String(fw_version) + "\",";
        json += "\"ts\":" + String(ts) + ",";
        json += "\"tds_in_ppm\":" + String(s.getTDS1()) + ",";
        json += "\"tds_out_ppm\":" + String(s.getTDS2());
        json += "}";

        mqttClient.publish(baseTopic("quality").c_str(), json.c_str());
    }

    // ================= STATE =================
    static String lastStateSent = "";

    String currentState = String(c.getStateName());

    if (currentState != lastStateSent) {

        lastStateSent = currentState;

        String json = "{";
        json += "\"device_id\":\"" + device_id + "\",";
        json += "\"ts\":" + String(ts) + ",";
        json += "\"state\":\"" + currentState + "\",";
        json += "\"running\":" + String(c.isRunning()) + ",";
        json += "\"retry\":" + String(c.getRetryCount());
        json += "}";

        mqttClient.publish(baseTopic("state").c_str(), json.c_str());
    }

    // ================= OUTPUTS =================
    static OutputsState lastOut = {0};

    OutputsState out = c.getOutputs();

    if (memcmp(&out, &lastOut, sizeof(out)) != 0) {

        lastOut = out;

        String json = "{";
        json += "\"device_id\":\"" + device_id + "\",";
        json += "\"ts\":" + String(ts) + ",";
        json += "\"pump_low\":" + String(out.pumpLow) + ",";
        json += "\"pump_high\":" + String(out.pumpHigh) + ",";
        json += "\"pump_inlet\":" + String(out.pumpInlet) + ",";
        json += "\"pump_dose\":" + String(out.pumpDose) + ",";
        json += "\"valve_flush\":" + String(out.valveFlush) + ",";
        json += "\"valve_inlet\":" + String(out.valveInlet);
        json += "}";

        mqttClient.publish(baseTopic("outputs").c_str(), json.c_str());
    }

    // ================= INPUTS =================
    static String lastInputs = "";

    String inputs = String(s.getD1()) + String(s.getD2()) + String(s.getD3()) +
                    String(s.getD4()) + String(s.getD5()) + String(s.getD6());

    if (inputs != lastInputs) {

        lastInputs = inputs;

        String json = "{";
        json += "\"device_id\":\"" + device_id + "\",";
        json += "\"ts\":" + String(ts) + ",";
        json += "\"demand\":" + String(s.getD1()) + ",";
        json += "\"crudo_ok\":" + String(s.getD2()) + ",";
        json += "\"dose_ok\":" + String(s.getD3()) + ",";
        json += "\"presostato\":" + String(s.getD4()) + ",";
        json += "\"reserva1\":" + String(s.getD5()) + ",";
        json += "\"reserva2\":" + String(s.getD6());
        json += "}";

        mqttClient.publish(baseTopic("inputs").c_str(), json.c_str());
    }

    // ================= HEARTBEAT =================
    if (now - lastHeartbeat > 30000) {

        lastHeartbeat = now;

        String json = "{";
        json += "\"device_id\":\"" + device_id + "\",";
        json += "\"ts\":" + String(ts) + ",";
        json += "\"status\":\"online\"";
        json += "}";

        mqttClient.publish(baseTopic("heartbeat").c_str(), json.c_str());
    }

    // ================= CMD ACK =================
    // Published after control.update() has set the pending ACK this iteration.
    // Uses snprintf into a stack buffer — no heap, no String.
    // clearAck() is called ONLY on successful publish. If publish() fails
    // (disconnect, buffer full, TCP error), the ACK stays in RAM and is
    // retried on the next loop iteration, preventing silent TIMEOUT on backend.
    if (cmds.hasPendingAck() && mqttClient.connected()) {
        const PendingAck& ack = cmds.getPendingAck();
        char json[256];
        snprintf(json, sizeof(json),
            "{\"command_id\":\"%s\",\"device_id\":\"%s\","
            "\"cmd\":\"%s\",\"ack\":\"%s\",\"reason\":\"%s\",\"ts\":%ld}",
            ack.command_id,
            device_id.c_str(),
            Commands::cmdToString(ack.cmd_type),
            Commands::ackStatusToString(ack.status),
            ack.reason,
            ts);
        bool ok = mqttClient.publish(baseTopic("cmd/ack").c_str(), json);
        if (ok) {
            cmds.clearAck();
        } else {
            Serial.println("[CMD] ACK publish failed, retry next loop");
        }
    }
}