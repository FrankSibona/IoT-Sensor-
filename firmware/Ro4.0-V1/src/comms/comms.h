#pragma once

#include <WiFi.h>
#include <WiFiManager.h>
#include <PubSubClient.h>

class Sensors;
class Control;

class Comms {
public:
    void begin();
    void update(Sensors &s, Control &c);

private:
    bool sendSnapshot = false;

    void reconnect();
};