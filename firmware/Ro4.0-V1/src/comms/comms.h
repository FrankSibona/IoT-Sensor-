#pragma once

#include <WiFi.h>
#include <WiFiManager.h>
#include <PubSubClient.h>

class Sensors;
class Control;
class Commands;  // full definition in commands/commands.h, included by comms.cpp

class Comms {
public:
    void begin(Commands &cmds);
    void update(Sensors &s, Control &c, Commands &cmds);

private:
    bool sendSnapshot = false;

    void reconnect();
};