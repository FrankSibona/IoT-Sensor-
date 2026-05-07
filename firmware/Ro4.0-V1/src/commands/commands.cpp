#include "commands.h"

void Commands::update(){
    //Esto viene de MQTT (lo llamaremos desde comms)
}
CommandType Commands::getCommand(){
    return current;
}

void Commands::clear(){
    current = CMD_NONE;
}
void Commands::setCommand(CommandType cmd){
    current = cmd;
}