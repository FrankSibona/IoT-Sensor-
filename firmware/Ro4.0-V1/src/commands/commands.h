#pragma once
#include <Arduino.h>

// ============================================================
// Commands — MQTT command ingestion and ACK serialization
//
// ROLE: pure data container + parsing. No MQTT calls, no FSM logic.
//
//   Interaction points:
//     main.cpp    → declares Commands as global, calls begin()
//     comms.cpp   → calls receive() from MQTT callback
//                   reads hasPendingAck / getPendingAck / clearAck
//     control.cpp → reads hasPending / getPending / hasExpired
//                   calls setPendingAck() BEFORE clearPending()
//                   calls clearPending()
//
// MVP GUARANTEES AND LIMITATIONS:
//   _last_id is stored in RAM only — cleared on every reboot.
//   Idempotence is best-effort: a command that executed before a
//   reboot may execute again if re-delivered and FSM allows it.
//   Exactly-once execution across reboots is NOT guaranteed.
//
//   FSM protections (in control.cpp) make this safe in practice:
//     START re-delivered in IDLE  → normal start, physically safe
//     STOP  re-delivered in IDLE  → REJECTED (invalid FSM state)
//     FLUSH re-delivered in IDLE  → REJECTED (invalid FSM state)
//     RST   re-delivered in IDLE  → REJECTED ("already_idle")
//       RST is accepted only in FAULT, STARTING, FLUSHING.
//       Rejecting RST in IDLE prevents future side-effects if reset
//       behavior evolves beyond the current no-op semantics.
//
//   No queues, no NVS persistence, no retry logic in this module.
// ============================================================

// ===================== ENUMS =====================
// All scoped with explicit uint8_t underlying type:
//   - no implicit integer conversions
//   - deterministic 1-byte storage
//   - strong type safety across FSM and MQTT boundaries

enum class CommandType : uint8_t {
    NONE  = 0,
    START = 1,
    STOP  = 2,
    FLUSH = 3,
    RST   = 4,
};

// Internal representation of command outcomes.
// Converted to MQTT strings only at serialization time in comms.cpp.
// Keeps wire format separated from internal state.
enum class AckStatus : uint8_t {
    EXECUTED = 0,
    REJECTED = 1,
};

// Result of receive(). Each value has distinct operational meaning:
//   ACCEPTED       → command stored, FSM will process it next loop
//   DUPLICATE      → same command_id already queued or processed (idempotence)
//   BUSY           → another command is pending, slot occupied
//   INVALID_JSON   → malformed payload or missing required fields
//   UNKNOWN_COMMAND→ cmd string not in {START, STOP, FLUSH, RST}
//                    indicates a backend/firmware contract mismatch
enum class ReceiveResult : uint8_t {
    ACCEPTED        = 0,
    DUPLICATE       = 1,
    BUSY            = 2,
    INVALID_JSON    = 3,
    UNKNOWN_COMMAND = 4,
};

// ===================== DATA STRUCTURES =====================
// Fixed-size only. No heap allocation, no String class.
//
// PendingCmd  → one incoming command slot
//   Written:  receive()        (comms.cpp MQTT callback)
//   Read:     getPending()     (control.cpp)
//   Cleared:  clearPending()   (control.cpp, after FSM decision)
//
// PendingAck  → one outgoing ACK slot
//   Written:  setPendingAck()  (control.cpp, after FSM decision)
//   Read:     getPendingAck()  (comms.cpp, for publish)
//   Cleared:  clearAck()       (comms.cpp, after publish)

struct PendingCmd {
    char        id[37];    // UUID4 "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\0"
    CommandType type;      // 1 byte; NONE when slot is empty
    uint32_t    deadline;  // Unix timestamp from deadline_at; 0 = no expiry
    bool        active;    // true = slot occupied
};

struct PendingAck {
    char        command_id[37];  // mirrors the processed PendingCmd.id
    CommandType cmd_type;        // serialized to string by comms.cpp
    AckStatus   status;          // serialized to string by comms.cpp
    char        reason[33];      // reject reason, max 32 chars + null
    bool        active;          // true = ACK waiting to be published
};

// ===================== COMMANDS CLASS =====================

class Commands {
public:
    void begin();

    // ── Ingestion (called by comms.cpp from MQTT callback) ────────────
    //
    // Parses payload and stores the command if all checks pass.
    // payload points into PubSubClient's internal buffer — valid only
    // for the duration of the callback. All bytes needed are copied
    // into fixed storage before this function returns.
    // Does NOT call mqttClient.publish() or any MQTT function.
    //
    // Caller (comms.cpp) should log or count non-ACCEPTED results
    // for observability. Each result maps to a distinct failure mode.
    ReceiveResult receive(const byte* payload, unsigned int length);

    // ── PendingCmd accessors (consumed by control.cpp) ────────────────

    bool              hasPending()  const;
    const PendingCmd& getPending()  const;

    // Returns true if now > _pending.deadline.
    // Returns false if deadline is 0 or NTP is not synced.
    // When NTP is unsynced, prefers executing over silently dropping.
    bool              hasExpired()  const;

    // Saves _pending.id into _last_id for RAM-level idempotence,
    // then resets _pending to its initial state.
    //
    // ORDERING INVARIANT: call setPendingAck() BEFORE clearPending().
    // After clearPending(), the memory referenced by getPending() is reset.
    void              clearPending();

    // ── PendingAck (written by control.cpp, read by comms.cpp) ────────

    // Records the ACK result from FSM processing.
    // reason may be nullptr or "" when status is EXECUTED.
    void setPendingAck(const char* id, CommandType type,
                       AckStatus status, const char* reason);

    bool              hasPendingAck()  const;
    const PendingAck& getPendingAck()  const;

    // Resets _ack. Called by comms.cpp after publish() returns.
    void              clearAck();

    // ── Serialization helpers ─────────────────────────────────────────
    // cmdFromString     : used internally by receive() (JSON → enum)
    // cmdToString       : used by comms.cpp to build ACK JSON
    // ackStatusToString : used by comms.cpp to build ACK JSON

    static CommandType  cmdFromString(const char* s);
    static const char*  cmdToString(CommandType t);
    static const char*  ackStatusToString(AckStatus s);

private:
    PendingCmd  _pending;     // ~48 bytes with alignment padding
    PendingAck  _ack;         // ~76 bytes with alignment padding
    char        _last_id[37]; // RAM-only: last processed id (best-effort dedup)

    // Returns true if id matches _pending.id or _last_id.
    // Uses bounded comparison — no strlen on untrusted input.
    bool isDuplicate(const char* id) const;
};
