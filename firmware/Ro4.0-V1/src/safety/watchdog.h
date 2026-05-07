#include <esp_task_wdt.h>

void watchdogInit(){
    esp_task_wdt_init(5,true);
    esp_task_wdt_add(NULL);
}

void watchdogReset(){
    esp_task_wdt_reset();
}
