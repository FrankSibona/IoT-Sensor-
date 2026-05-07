#include "sensors.h"
#include <config.h>

// ================= STATIC =================
volatile unsigned long Sensors::pulsesQ1 = 0;
volatile unsigned long Sensors::pulsesQ2 = 0;

// ================= ISR =================
void IRAM_ATTR Sensors::isrQ1() { pulsesQ1++; }
void IRAM_ATTR Sensors::isrQ2() { pulsesQ2++; }

// ================= NVS =================
void Sensors::loadTotals() {
    prefs.begin("kairox", true);

    totalPerm = prefs.getFloat("perm", 0);
    totalRech = prefs.getFloat("rech", 0);

    prefs.end();

    lastSavedPerm = totalPerm;
    lastSavedRech = totalRech;

    Serial.println("📦 Totales cargados");
}

void Sensors::saveTotals() {
    prefs.begin("kairox", false);

    prefs.putFloat("perm", totalPerm);
    prefs.putFloat("rech", totalRech);

    prefs.end();

    lastSavedPerm = totalPerm;
    lastSavedRech = totalRech;

    Serial.println("💾 Totales guardados");
}

// ================= INIT =================
void Sensors::begin() {

    pinMode(PIN_D1, INPUT_PULLDOWN);
    pinMode(PIN_D2, INPUT_PULLDOWN);
    pinMode(PIN_D3, INPUT_PULLDOWN);
    pinMode(PIN_D4, INPUT_PULLDOWN);
    pinMode(PIN_D5, INPUT_PULLDOWN);
    pinMode(PIN_D6, INPUT_PULLDOWN);

    pinMode(PIN_Q1, INPUT_PULLDOWN);
    pinMode(PIN_Q2, INPUT_PULLDOWN);

    attachInterrupt(digitalPinToInterrupt(PIN_Q1), isrQ1, RISING);
    attachInterrupt(digitalPinToInterrupt(PIN_Q2), isrQ2, RISING);

    analogReadResolution(12);
    analogSetAttenuation(ADC_11db);

    loadTotals();
}

// ================= UPDATE =================
void Sensors::update() {

    // -------- DIGITALES --------
    d1 = digitalRead(PIN_D1);
    d2 = digitalRead(PIN_D2);
    d3 = digitalRead(PIN_D3);
    d4 = digitalRead(PIN_D4);
    d5 = digitalRead(PIN_D5);
    d6 = digitalRead(PIN_D6);

    // -------- CAUDAL --------
    if (millis() - lastFlowTime >= 1000) {

        noInterrupts();
        unsigned long p1_local = pulsesQ1;
        unsigned long p2_local = pulsesQ2;
        pulsesQ1 = 0;
        pulsesQ2 = 0;
        interrupts();

        flow1 = (p1_local / FLOW_K) * 60.0;
        flow2 = (p2_local / FLOW_K) * 60.0;

        totalPerm += flow1 / 60.0;
        totalRech += flow2 / 60.0;

        lastFlowTime = millis();
    }

    // -------- SAVE INTELIGENTE --------
    bool tiempo = millis() - lastSaveTime > 3600000; // 1 hora
    bool volumen = (totalPerm - lastSavedPerm) > 50; //cada 50lts

    if (tiempo || volumen) {
        saveTotals();
        lastSaveTime = millis();
    }

    // -------- ANALÓGICOS --------

    // ===== PRESIÓN (EWMA) =====
    float alpha = 0.3;

    float p1_raw = (analogRead(PIN_AIN0) / 4095.0) * 10.0;
    float p2_raw = (analogRead(PIN_AIN1) / 4095.0) * 10.0;

    // inicialización (evita arranque en 0)
    if (p1_f == 0) p1_f = p1_raw;
    if (p2_f == 0) p2_f = p2_raw;

    // filtro EWMA
    p1_f = alpha * p1_raw + (1 - alpha) * p1_f;
    p2_f = alpha * p2_raw + (1 - alpha) * p2_f;

    p1 = p1_f;
    p2 = p2_f;


    // ===== TDS (MEDIANA DE 3) =====
    float t1 = (analogRead(PIN_TDS1) / 4095.0) * 5.0;
    float t2 = (analogRead(PIN_TDS2) / 4095.0) * 5.0;

    // buffer circular
    tds1_buf[tds_index] = t1;
    tds2_buf[tds_index] = t2;

    tds_index = (tds_index + 1) % 3;

    // función mediana inline
    auto median3 = [](float a, float b, float c) {
        if ((a <= b && b <= c) || (c <= b && b <= a)) return b;
        if ((b <= a && a <= c) || (c <= a && a <= b)) return a;
        return c;
    };

    // aplicar filtro
    tds1 = median3(tds1_buf[0], tds1_buf[1], tds1_buf[2]);
    tds2 = median3(tds2_buf[0], tds2_buf[1], tds2_buf[2]);

}

// ================= GETTERS =================
float Sensors::getFlow1() { return flow1; }
float Sensors::getFlow2() { return flow2; }
float Sensors::getPressure1() { return p1; }
float Sensors::getPressure2() { return p2; }
float Sensors::getTDS1() { return tds1; }
float Sensors::getTDS2() { return tds2; }

float Sensors::getTotalPerm() { return totalPerm; }
float Sensors::getTotalRech() { return totalRech; }

bool Sensors::getD1() { return d1; }
bool Sensors::getD2() { return d2; }
bool Sensors::getD3() { return d3; }
bool Sensors::getD4() { return d4; }
bool Sensors::getD5() { return d5; }
bool Sensors::getD6() { return d6; }

// 👇 SÍ, mantenelos (no es redundancia, es semántica)
bool Sensors::getDemand() { return d1; }
bool Sensors::getCrudoOK() { return d2; }
bool Sensors::getDoseOK() { return d3; }
bool Sensors::getPresostato() { return d4; }

// lógica
bool Sensors::demanda() { return d1; }
bool Sensors::crudoDisponible() { return d2; }
bool Sensors::presionOK() { return d4; }