# Flink Delayed Order SMS Job

This module contains the Apache Flink job responsible for detecting delayed orders and emitting SMS command events.

The job consumes full order state snapshots from the compacted Kafka topic `Orders`, keeps state per order, registers processing-time timers based on `expectedDeliveryTime`, and emits an idempotent `SEND_DELAY_SMS` command when an order becomes delayed.

---

## Purpose

Detect orders that pass their expected delivery time without reaching a terminal state.

An order is considered delayed when:

```text
current processing time >= expectedDeliveryTime
AND order status is not DELIVERED
AND order status is not CANCELLED
AND delay SMS has not already been emitted
```

---

## Input Topic

```text
Orders
```

The `Orders` topic is a compacted Kafka topic.

Each message must be keyed by:

```text
orderId
```

Each message value contains the full latest order state.

Example input:

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "storeId": "store-001",
  "status": "ACCEPTED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T18:45:00Z",
  "lastUpdatedAt": "2026-05-12T18:45:10Z",
  "eventTime": "2026-05-12T18:45:10Z",
  "stateLogs": [
    {
      "status": "CREATED",
      "at": "2026-05-12T18:45:00Z"
    },
    {
      "status": "ACCEPTED",
      "at": "2026-05-12T18:45:10Z"
    }
  ],
  "schemaVersion": 1
}
```

---

## Output Topic

```text
sms-commands
```

When an order is detected as delayed, the job emits a command event.

Example output:

```json
{
  "commandId": "ord-001:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS",
  "orderId": "ord-001",
  "customerId": "cus-001",
  "storeId": "store-001",
  "reason": "ORDER_DELAYED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T19:31:00Z",
  "schemaVersion": 1
}
```

The Kafka message key for this output should be:

```text
commandId
```

---

## Processing Model

The job uses the Flink DataStream API.

Pipeline:

```text
Kafka Source: Orders
        |
        v
Deserialize JSON
        |
        v
Validate Order State
        |
        v
keyBy(orderId)
        |
        v
KeyedProcessFunction
        |
        v
Kafka Sink: sms-commands
```

---

## Time Semantics

The job uses **Processing Time** timers for delay detection.

Reason:

The business question is based on wall-clock time:

```text
Has the real current time passed the expectedDeliveryTime?
```

Therefore, the delay timer is registered using processing time.

The input still includes `eventTime` for observability, debugging, stale update analysis, and future event-time use cases.

---

## State Model

The job keeps one keyed state per `orderId`.

State fields:

```text
orderId
customerId
storeId
currentStatus
expectedDeliveryTime
lastUpdatedAt
delaySmsEmitted
registeredTimerTime
```

Suggested Flink state:

```text
ValueState<OrderDelayState>
```

---

## Supported Statuses

```text
CREATED
ACCEPTED
PICKED_UP
DELIVERED
CANCELLED
```

Terminal statuses:

```text
DELIVERED
CANCELLED
```

No delay SMS should be emitted for terminal orders.

---

## Event Handling Rules

### New or Updated Order State

For every input message:

1. Parse the JSON payload.
2. Validate required fields.
3. Key the stream by `orderId`.
4. Load current Flink state for that order.
5. Ignore stale updates based on `lastUpdatedAt`.
6. Update Flink state with the latest order state.

---

### Active Order

If the order is active:

```text
status != DELIVERED
status != CANCELLED
```

Then:

- If `expectedDeliveryTime` is in the future:
  - register or update a processing-time timer.
- If `expectedDeliveryTime` is already in the past:
  - emit delay SMS immediately if not already emitted.

---

### Delivered Order

If the latest state has:

```text
status = DELIVERED
```

Then:

- update state
- delete any registered delay timer
- do not emit delay SMS

---

### Cancelled Order

If the latest state has:

```text
status = CANCELLED
```

Then:

- update state
- delete any registered delay timer
- do not emit delay SMS

---

### ETA Updated Order

ETA updates are represented as a new full order state with an updated:

```text
expectedDeliveryTime
```

The job should:

- compare the new deadline with the currently registered timer
- delete the old timer if needed
- register a new timer based on the latest `expectedDeliveryTime`

---

### Duplicate Events

Duplicate messages may be received from Kafka or the simulator.

The job must not emit duplicate SMS commands.

Duplicate prevention is handled by:

```text
delaySmsEmitted = true
```

in Flink state.

---

### Out-of-Order Updates

If an incoming order state has `lastUpdatedAt` older than the state already stored in Flink, it is considered stale.

Stale updates should be ignored.

---

## Timer Behavior

When the processing-time timer fires:

1. Load the order state.
2. Check the order is not terminal.
3. Check `delaySmsEmitted = false`.
4. Check current timer timestamp is greater than or equal to `expectedDeliveryTime`.
5. Emit `SEND_DELAY_SMS`.
6. Set `delaySmsEmitted = true`.

---

## Idempotency

The output command ID must be deterministic:

```text
commandId = orderId + ":DELAY_SMS"
```

Example:

```text
ord-001:DELAY_SMS
```

This allows downstream services to deduplicate commands before sending real SMS messages.

The Flink job also stores:

```text
delaySmsEmitted = true
```

to avoid emitting multiple commands for the same order.

---

## Configuration

The job supports CLI arguments and environment variables.

| Config | Environment Variable | Default |
|---|---|---|
| `kafka.bootstrap.servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `orders.topic` | `ORDERS_TOPIC` | `Orders` |
| `sms.commands.topic` | `SMS_COMMANDS_TOPIC` | `sms-commands` |
| `consumer.group.id` | `CONSUMER_GROUP_ID` | `delayed-order-sms-flink` |
| `checkpoint.storage.path` | `CHECKPOINT_STORAGE_PATH` | `file:///tmp/flink-checkpoints` |
| `checkpoint.interval.ms` | `CHECKPOINT_INTERVAL_MS` | `10000` |
| `parallelism` | `PARALLELISM` | `1` |
| `restart.attempts` | `RESTART_ATTEMPTS` | `3` |
| `restart.delay.ms` | `RESTART_DELAY_MS` | `5000` |

Example:

```bash
flink run \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  target/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers localhost:9092 \
  --orders.topic Orders \
  --sms.commands.topic sms-commands \
  --consumer.group.id delayed-order-sms-flink \
  --checkpoint.storage.path file:///tmp/flink-checkpoints \
  --parallelism 1
```

---

## Build

From this directory:

```bash
mvn clean package
```

Expected output:

```text
target/delayed-order-sms-flink-job.jar
```

From the repository root, if Makefile is configured:

```bash
make build-flink-job
```

---

## Run Locally with Docker Flink

Start local infrastructure from the repository root:

```bash
docker compose up -d
```

Build the job:

```bash
cd flink-job
mvn clean package
```

Submit the job to the local Flink container:

```bash
docker exec -it flink-jobmanager flink run \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  /opt/flink/usrlib/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --sms.commands.topic sms-commands \
  --consumer.group.id delayed-order-sms-flink \
  --checkpoint.storage.path file:///opt/flink/checkpoints \
  --parallelism 1
```

Flink UI:

```text
http://localhost:8081
```

Kafka UI:

```text
http://localhost:8080
```

---

## Test with Simulator

Run the simulator from the project root or simulator directory.

Example delayed order test:

```bash
cd simulator

PYTHONPATH=src python -m order_simulator.main \
  --scenario delayed-orders \
  --orders-count 1 \
  --kafka-bootstrap-servers localhost:9092 \
  --orders-topic Orders
```

Expected result:

```text
One SEND_DELAY_SMS command should be emitted to sms-commands after expectedDeliveryTime passes.
```

Example on-time order test:

```bash
PYTHONPATH=src python -m order_simulator.main \
  --scenario on-time-orders \
  --orders-count 1 \
  --kafka-bootstrap-servers localhost:9092 \
  --orders-topic Orders
```

Expected result:

```text
No SEND_DELAY_SMS command should be emitted.
```

---

## Verify Output

Consume from `sms-commands`:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic sms-commands \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

Expected key for delayed order:

```text
ord-delayed-orders-0001:DELAY_SMS
```

Expected value:

```json
{
  "commandId": "ord-delayed-orders-0001:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS",
  "orderId": "ord-delayed-orders-0001",
  "customerId": "cus-0001",
  "storeId": "store-001",
  "reason": "ORDER_DELAYED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T19:31:00Z",
  "schemaVersion": 1
}
```

---

## Failure Recovery Test

The job should support checkpoint-based recovery.

Suggested test flow:

```text
1. Start Kafka and Flink.
2. Submit the Flink job.
3. Run the failure-recovery simulator scenario.
4. Ensure orders are consumed and timers are registered.
5. Restart or fail the Flink job before expectedDeliveryTime.
6. Restore/restart the job.
7. Wait until expectedDeliveryTime passes.
8. Verify only one SEND_DELAY_SMS command is emitted per delayed order.
```

Example simulator command:

```bash
cd simulator

SCENARIO=failure-recovery \
ORDERS_COUNT=1 \
PAUSE_AFTER_STEP=2 \
PAUSE_DURATION_SECONDS=60 \
PYTHONPATH=src python -m order_simulator.main
```

---

## Current Behavior

This implementation is intentionally minimal.

It currently:

- consumes from `Orders`
- parses full order state snapshots
- ignores invalid JSON
- ignores stale updates using `lastUpdatedAt`
- uses `keyBy(orderId)`
- uses `ValueState<OrderDelayState>`
- uses processing-time timers
- deletes old timers when the order becomes terminal
- updates timers when `expectedDeliveryTime` changes
- emits at most one SMS command per order
- writes to `sms-commands`

---

## Limitations

Current limitations:

- Invalid events are logged but not yet sent to `dead-letter-events`.
- Unit tests are minimal initially.
- No custom metrics are implemented yet.
- State TTL is not configured yet.
- Replay and dry-run behavior are not implemented in the Flink job.
- Production deployment strategy is not included in this module.

---

## Next Improvements

Planned improvements:

1. Add DLQ output for invalid events.
2. Add unit tests using Flink test harness.
3. Add state TTL.
4. Add custom metrics:
   - delayed orders detected
   - SMS commands emitted
   - stale updates ignored
   - invalid messages
   - active timers
5. Add savepoint compatibility documentation.
6. Add end-to-end test automation.
7. Add replay-safe mode for historical reprocessing.

---

## Definition of Done

This module is ready when:

- The job builds successfully.
- The job consumes from `Orders`.
- The job writes to `sms-commands`.
- Delayed orders emit one SMS command.
- On-time orders emit no SMS command.
- Cancelled orders emit no SMS command.
- Duplicate updates do not emit duplicate SMS commands.
- Out-of-order stale updates are ignored.
- ETA updates change timer behavior.
- Checkpointing is enabled.
- Failure recovery is manually validated.