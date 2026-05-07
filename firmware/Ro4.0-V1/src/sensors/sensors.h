#pragma once

#include <Arduino.h>
#include <Preferences.h>

class Sensors {
public:
    void begin();
    void update();

    float getFlow1();
    float getFlow2();
    float getPressure1();
    float getPressure2();
    float getTDS1();
    float getTDS2();

    float getTotalPerm();
    float getTotalRech();

    bool getD1();
    bool getD2();
    bool getD3();
    bool getD4();
    bool getD5();
    bool getD6();

    // 👇 NOMBRADOS (sí, mantenelos)
    bool getDemand();
    bool getCrudoOK();
    bool getDoseOK();
    bool getPresostato();

    bool demanda();
    bool crudoDisponible();
    bool presionOK();

private:
    float flow1 = 0;
    float flow2 = 0;
    float p1 = 0;
    float p2 = 0;
    float tds1 = 0;
    float tds2 = 0;

    float totalPerm = 0;
    float totalRech = 0;

    float lastSavedPerm = 0;
    float lastSavedRech = 0;

    unsigned long lastSaveTime = 0;

    bool d1, d2, d3, d4, d5, d6;

    Preferences prefs;

    void saveTotals();
    void loadTotals();

    static void IRAM_ATTR isrQ1();
    static void IRAM_ATTR isrQ2();

    static volatile unsigned long pulsesQ1;
    static volatile unsigned long pulsesQ2;

    unsigned long lastFlowTime = 0;

    // ===== FILTROS =====

    // presión (EWMA)
    float p1_f = 0;
    float p2_f = 0;

    // TDS (mediana)
    float tds1_buf[3] = {0};
    float tds2_buf[3] = {0};
    int tds_index = 0;
};