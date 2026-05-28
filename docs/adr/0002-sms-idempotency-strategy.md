# ADR-0002: SMS Idempotency Strategy

**Status:** Accepted

**Date:** 2025-01-20

**Deciders:** Delayed Order SMS Team

## Context

The Delayed Order SMS Flink job emits `SendDelaySmsCommand` messages to Kafka topic `sms-commands` when an order is detected as delayed. The downstream SMS service reads from this topic and sends actual text messages to customers.

**Problem:** In a distributed streaming system with at-least-once semantics, the same SMS command may be emitted multiple times:

1. **Flink checkpoint/restart cycles** — after a failure, events may be replayed from the last checkpoint, potentially triggering the same timer again.
2. **Kafka at-least-once delivery** — the Flink Kafka producer guarantees at-least-once, meaning a command could be written to Kafka more than once.
3. **Downstream consumer retries** — the SMS service may re-consume messages after a failure.

Sending duplicate SMS messages to customers is unacceptable — it degrades user experience and incurs unnecessary cost.

### Options Considered

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A. Exactly-once Kafka** | Use Kafka transactions for exactly-once semantics | Strongest guarantee | Requires transactional Kafka setup; downstream must also use transactions |
| **B. Deduplication in Flink** | Maintain a set of emitted command IDs in Flink state | Simple, in-process | State grows unboundedly |
| **C. Idempotency key + downstream dedup** | Embed a unique `commandId` in each command; downstream deduplicates | Stateless Flink; separation of concerns | Requires downstream cooperation |
| **D. Idempotency key + Flink boolean flag** | Use `commandId = orderId + ":DELAY_SMS"` + `delaySmsEmitted` boolean | Simple; prevents duplicate within Flink lifecycle | Doesn't protect across complete job restarts |

## Decision

**Combination of Option C and D:**

1. **Unique `commandId`:** Each `SmsCommand` carries a deterministic, unique `commandId` formed by concatenating the `orderId` with a literal suffix: `"{orderId}:DELAY_SMS"`. This is the idempotency key.

2. **Flink-level guard:** A `delaySmsEmitted` boolean in `OrderDelayState` prevents the same order from emitting an SMS more than once within the same Flink job lifecycle. This covers timer re-firing, stale updates, and most at-least-once replay scenarios.

3. **Downstream deduplication:** The SMS service is required to deduplicate by `commandId`. Any command with a previously-seen `commandId` is silently ignored.

### Implementation Detail

```java
// In DelayedOrderProcessFunction
public static SmsCommand delaySms(OrderDelayState state, Instant now) {
    return new SmsCommand(
        state.getOrderId() + ":DELAY_SMS",  // commandId
        "SEND_DELAY_SMS",
        state.getOrderId(),
        state.getCustomerId(),
        state.getStoreId(),
        "ORDER_DELAYED",
        state.getExpectedDeliveryTime(),
        now
    );
}
```

The `delaySmsEmitted` boolean is set to `true` immediately after emitting, and checked before any emission attempt.

## Consequences

### Positive
- **Idempotent by key:** The same `orderId` always produces the same `commandId`, making deduplication trivial at the downstream.
- **No unbounded state growth:** A single boolean per active order (orders that have not yet been delivered/cancelled and not yet had their SMS emitted).
- **Separation of concerns:** Flink handles the "should we emit?" decision; the SMS service handles the "have we already sent this?" decision.
- **At-least-once is safe:** Even if Kafka delivers the message multiple times or Flink replays events, no customer receives duplicate SMS.

### Negative
- **Requires downstream cooperation:** The SMS service must implement deduplication logic. This is documented in the `sms-commands` topic contract.
- **State TTL needed:** Orders in terminal state must eventually be cleaned up to prevent state growth. See TTL configuration (`--state-ttl-days`).
- **Doesn't cover delete-and-recreate scenarios:** If the Flink job is completely deleted and recreated from scratch (no savepoint), all `delaySmsEmitted` booleans are lost. In this case, the downstream deduplication is the only safeguard.

## References

- [Kafka Idempotent Producer](https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence)
- ADR-0001: Processing Time vs Event Time
- `sms-commands` topic schema: `schemas/sms-commands/send-delay-sms-command.schema.json`