#include "commands.h"
#include <string.h>
#include <time.h>
#include <ArduinoJson.h>

// ===================== INIT =====================

void Commands::begin() {
    // Explicit field initialization — no memset on enum class fields.
    // char arrays: set first byte to '\0' (treated as empty strings).
    _pending.id[0]    = '\0';
    _pending.type     = CommandType::NONE;
    _pending.deadline = 0;
    _pending.active   = false;

    _ack.command_id[0] = '\0';
    _ack.cmd_type      = CommandType::NONE;
    _ack.status        = AckStatus::EXECUTED;
    _ack.reason[0]     = '\0';
    _ack.active        = false;

    _last_id[0] = '\0';
}

// ===================== SERIALIZATION HELPERS =====================

CommandType Commands::cmdFromString(const char* s) {
    if (!s)                      return CommandType::NONE;
    if (strcmp(s, "START") == 0) return CommandType::START;
    if (strcmp(s, "STOP")  == 0) return CommandType::STOP;
    if (strcmp(s, "FLUSH") == 0) return CommandType::FLUSH;
    if (strcmp(s, "RST")   == 0) return CommandType::RST;
    return CommandType::NONE;
}

const char* Commands::cmdToString(CommandType t) {
    switch (t) {
        case CommandType::START: return "START";
        case CommandType::STOP:  return "STOP";
        case CommandType::FLUSH: return "FLUSH";
        case CommandType::RST:   return "RST";
        default:                 return "";
    }
}

const char* Commands::ackStatusToString(AckStatus s) {
    switch (s) {
        case AckStatus::EXECUTED: return "EXECUTED";
        case AckStatus::REJECTED: return "REJECTED";
        default:                  return "";
    }
}

// ===================== PRIVATE HELPERS =====================

bool Commands::isDuplicate(const char* id) const {
    if (!id) return false;
    // Already queued: same command received twice before processing
    if (_pending.active && strncmp(_pending.id, id, 36) == 0) return true;
    // Already processed: backend retry after EXECUTED/REJECTED
    if (_last_id[0] != '\0' && strncmp(_last_id, id, 36) == 0) return true;
    return false;
}

// ===================== INGESTION =====================

ReceiveResult Commands::receive(const byte* payload, unsigned int length) {
    // Stack-allocated: no heap, deterministic lifetime.
    // Zero-copy parse: ArduinoJson stores string pointers into payload.
    // We copy all needed values out before returning — payload is only
    // valid for the duration of the callback.
    StaticJsonDocument<256> doc;

    if (deserializeJson(doc, payload, length) != DeserializationError::Ok) {
        return ReceiveResult::INVALID_JSON;
    }

    const char* id      = doc["command_id"] | (const char*)nullptr;
    const char* cmd_str = doc["cmd"]         | (const char*)nullptr;
    uint32_t    deadline = doc["deadline_at"] | (uint32_t)0;

    // Validate required fields. UUID4 is exactly 36 chars; strnlen bounds
    // the scan to avoid scanning past the end of a malformed payload.
    if (!id || !cmd_str || strnlen(id, 37) != 36) {
        return ReceiveResult::INVALID_JSON;
    }

    CommandType type = cmdFromString(cmd_str);
    if (type == CommandType::NONE) {
        return ReceiveResult::UNKNOWN_COMMAND;
    }

    if (isDuplicate(id)) {
        return ReceiveResult::DUPLICATE;
    }

    if (_pending.active) {
        return ReceiveResult::BUSY;
    }

    // All checks passed. Copy into fixed-size storage.
    // From this point, payload pointer is no longer referenced.
    strlcpy(_pending.id, id, sizeof(_pending.id));
    _pending.type     = type;
    _pending.deadline = deadline;
    _pending.active   = true;

    return ReceiveResult::ACCEPTED;
}

// ===================== PENDING CMD =====================

bool Commands::hasPending() const {
    return _pending.active;
}

const PendingCmd& Commands::getPending() const {
    return _pending;
}

bool Commands::hasExpired() const {
    if (!_pending.active || _pending.deadline == 0) return false;
    struct tm timeinfo;
    // If NTP is not yet synced, getLocalTime returns false.
    // Prefer executing over silently dropping due to clock uncertainty.
    if (!getLocalTime(&timeinfo)) return false;
    return (uint32_t)mktime(&timeinfo) > _pending.deadline;
}

void Commands::clearPending() {
    // Save id before clearing so isDuplicate() catches re-delivery.
    if (_pending.active) {
        strlcpy(_last_id, _pending.id, sizeof(_last_id));
    }
    _pending.id[0]    = '\0';
    _pending.type     = CommandType::NONE;
    _pending.deadline = 0;
    _pending.active   = false;
}

// ===================== PENDING ACK =====================

void Commands::setPendingAck(const char* id, CommandType type,
                              AckStatus status, const char* reason) {
    strlcpy(_ack.command_id, id, sizeof(_ack.command_id));
    _ack.cmd_type = type;
    _ack.status   = status;
    strlcpy(_ack.reason, reason ? reason : "", sizeof(_ack.reason));
    _ack.active   = true;
}

bool Commands::hasPendingAck() const {
    return _ack.active;
}

const PendingAck& Commands::getPendingAck() const {
    return _ack;
}

void Commands::clearAck() {
    _ack.command_id[0] = '\0';
    _ack.cmd_type      = CommandType::NONE;
    _ack.status        = AckStatus::EXECUTED;
    _ack.reason[0]     = '\0';
    _ack.active        = false;
}
