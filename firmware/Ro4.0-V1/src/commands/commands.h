#pragma once 

enum CommandType {
    CMD_NONE,
    CMD_START,
    CMD_STOP,
    CMD_FLUSH,
    CMD_RESET
};


class Commands {
public:
    void update();
    CommandType getCommand();
    void clear();
    void setCommand(CommandType cmd);

private:
    CommandType  current = CMD_NONE;
};