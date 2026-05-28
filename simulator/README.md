# Order Event Simulator

The Order Event Simulator generates realistic order state updates and publishes them to Kafka for local and end-to-end testing of the delayed order detection pipeline.

This simulator follows the current order stream model:

- Source topic: `Orders`
- Topic type: compacted
- Kafka message key: `orderId`
- Kafka message value: full latest order state
- Every order update produces a new event/message

---

## Purpose

The simulator is used to validate the Flink pipeline with controlled, repeatable scenarios.

It supports:

- Generating a configurable number of orders.
- Publishing order state updates to Kafka.
- Simulating normal and edge-case order flows.
- Testing duplicate and out-of-order updates.
- Supporting Flink failure and recovery tests.
- Running in dry-run mode without Kafka.

---

## Topic Model

The simulator publishes to:

```text
Orders
```

The `Orders` topic is expected to be a compacted Kafka topic.

Each message must be keyed by:

```text
orderId
```

Each message value must contain the complete latest state of the order.

Example lifecycle:

```text
Order created   -> publish full order state with status CREATED
Order accepted  -> publish full order state with status ACCEPTED
Order picked up -> publish full order state with status PICKED_UP
Order delivered -> publish full order state with status DELIVERED
Order cancelled -> publish full order state with status CANCELLED
ETA updated     -> publish full order state with updated expectedDeliveryTime
```

The simulator does not publish partial/delta events.

---

## Event Contract

Each produced message value should follow this structure:

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

Required fields:

| Field | Description |
|---|---|
| `orderId` | Unique order identifier and Kafka message key |
| `customerId` | Customer identifier |
| `storeId` | Store identifier |
| `status` | Latest order status |
| `expectedDeliveryTime` | Current expected delivery deadline |
| `createdAt` | Order creation time |
| `lastUpdatedAt` | Time of the latest state update |
| `eventTime` | Business event time |
| `stateLogs` | Historical state changes for the order |
| `schemaVersion` | Schema version |

---

## Supported Statuses

The simulator currently supports these order statuses:

```text
CREATED
ACCEPTED
PICKED_UP
DELIVERED
CANCELLED
```

ETA changes are represented by publishing a new full order state with an updated `expectedDeliveryTime`.

---

## Project Structure

```text
simulator/
  README.md
  requirements.txt

  scenarios/
    README.md
    on-time-orders.json
    delayed-orders.json
    cancelled-orders.json
    eta-updated-orders.json
    duplicate-events.json
    out-of-order-updates.json
    failure-recovery.json
    mixed-orders.json

  src/
    order_simulator/
      __init__.py
      main.py
      config.py
      scenario_loader.py
      template_renderer.py
      time_utils.py
      kafka_producer.py
      runner.py
```

---

## Configuration

The simulator can be configured using environment variables or CLI arguments.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `ORDERS_TOPIC` | `Orders` | Target Kafka topic |
| `SCENARIO` | `mixed-orders` | Scenario file name without `.json` |
| `ORDERS_COUNT` | `100` | Number of orders to generate |
| `ORDER_ID_PREFIX` | `ord` | Prefix for generated order IDs |
| `EVENT_DELAY_MULTIPLIER` | `1.0` | Multiplier for delays defined in scenario files |
| `DRY_RUN` | `false` | Print messages instead of publishing to Kafka |
| `RANDOM_SEED` | `42` | Random seed for deterministic mixed scenarios |
| `MAX_WORKERS` | `10` | Number of concurrent workers |
| `PAUSE_AFTER_STEP` | `0` | Step number to pause after, mainly for failure tests |
| `PAUSE_DURATION_SECONDS` | `0` | Pause duration for failure tests |

Example:

```bash
SCENARIO=delayed-orders \
ORDERS_COUNT=100 \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
ORDERS_TOPIC=Orders \
PYTHONPATH=src python -m order_simulator.main
```

---

## CLI Arguments

The simulator also supports CLI arguments:

```bash
PYTHONPATH=src python -m order_simulator.main \
  --scenario delayed-orders \
  --orders-count 100 \
  --kafka-bootstrap-servers localhost:9092 \
  --orders-topic Orders
```

Available arguments:

| Argument | Description |
|---|---|
| `--scenario` | Scenario name from `simulator/scenarios` without `.json` |
| `--orders-count` | Number of orders to generate |
| `--kafka-bootstrap-servers` | Kafka bootstrap servers |
| `--orders-topic` | Kafka target topic |
| `--dry-run` | Print generated messages instead of publishing to Kafka |

---

## Installation

From the `simulator` directory:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Required dependency:

```text
confluent-kafka
```

---

## Running in Dry-Run Mode

Use dry-run mode to verify generated messages without Kafka.

```bash
PYTHONPATH=src python -m order_simulator.main \
  --scenario delayed-orders \
  --orders-count 2 \
  --dry-run
```

Expected output:

```json
{
  "topic": "Orders",
  "key": "ord-delayed-orders-0001",
  "value": {
    "orderId": "ord-delayed-orders-0001",
    "customerId": "cus-0001",
    "storeId": "store-001",
    "status": "CREATED",
    "expectedDeliveryTime": "2026-05-12T19:30:00Z",
    "createdAt": "2026-05-12T19:29:00Z",
    "lastUpdatedAt": "2026-05-12T19:29:00Z",
    "eventTime": "2026-05-12T19:29:00Z",
    "stateLogs": [
      {
        "status": "CREATED",
        "at": "2026-05-12T19:29:00Z"
      }
    ],
    "schemaVersion": 1
  }
}
```

---

## Running Against Kafka

Start local infrastructure from the repository root:

```bash
docker compose up -d
```

Wait ~30-60 seconds for all services, then verify:

```bash
docker compose ps
```

Then run the simulator:

```bash
cd simulator

PYTHONPATH=src python -m order_simulator.main \
  --scenario mixed-orders \
  --orders-count 100 \
  --kafka-bootstrap-servers localhost:9092 \
  --orders-topic Orders
```

### Convenience Script

From the project root, you can use `run_scenario.py` which wraps the simulator:

```bash
python run_scenario.py --scenario delayed-orders --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
```

This avoids needing to set `PYTHONPATH` or `cd` into the simulator directory.

### Quick Smoke Test (All Scenarios)

Run all scenarios with 5 orders each to validate the pipeline:

```bash
python run_scenario.py --scenario delayed-orders      --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario on-time-orders      --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario cancelled-orders     --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario duplicate-events     --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario eta-updated-orders   --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario mixed-orders         --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
```

### Practical Tips

- **Time expressions** like `now+2s` are resolved to real UTC timestamps at runtime. For `delayed-orders`, the scenario sets `expectedDeliveryTime: "now+2s"` so SMS appears ~2 seconds after events are published.
- **Kafka connection**: Use `localhost:9092` from the host, `kafka:29092` from inside Docker containers.
- **Error handling**: If you see `CoordinatorNotAvailableException`, it's a transient Kafka startup error that self-resolves.
- **Dry-run first**: Always validate a new scenario with `--dry-run` before publishing to Kafka.
- **Unique keys**: Each run generates deterministic IDs like `ord-delayed-orders-0001`. To avoid collisions between runs, restart the consumer group or use a different group ID.

---

## Supported Scenarios

Scenario files are located in:

```text
simulator/scenarios/
```

### 1. On-Time Orders

File:

```text
on-time-orders.json
```

Description:

Orders are created and delivered before `expectedDeliveryTime`.

Expected result:

```text
No delay SMS command should be emitted.
```

---

### 2. Delayed Orders

File:

```text
delayed-orders.json
```

Description:

Orders are created but are not delivered or cancelled before `expectedDeliveryTime`.

Expected result:

```text
One delay SMS command should be emitted per delayed order.
```

---

### 3. Cancelled Orders

File:

```text
cancelled-orders.json
```

Description:

Orders are created and cancelled before `expectedDeliveryTime`.

Expected result:

```text
No delay SMS command should be emitted.
```

---

### 4. ETA Updated Orders

File:

```text
eta-updated-orders.json
```

Description:

Orders receive a new `expectedDeliveryTime`.

Expected result:

```text
The Flink job should use the latest expectedDeliveryTime.
```

---

### 5. Duplicate Events

File:

```text
duplicate-events.json
```

Description:

The same full order state is published more than once.

Expected result:

```text
Duplicate order updates must not produce duplicate SMS commands.
```

---

### 6. Out-of-Order Updates

File:

```text
out-of-order-updates.json
```

Description:

Order snapshots are published in an unexpected order.

Expected result:

```text
The pipeline must not crash. Behavior should follow the documented business rules.
```

---

### 7. Failure Recovery

File:

```text
failure-recovery.json
```

Description:

Produces orders with future `expectedDeliveryTime` to support manual Flink failure and recovery testing.

Expected result:

```text
After Flink recovery, exactly one delay SMS command should be emitted per delayed order.
```

---

### 8. Mixed Orders

File:

```text
mixed-orders.json
```

Description:

Generates a configurable mix of all supported scenarios.

Expected result:

```text
SMS commands should only be emitted for orders that are actually delayed.
```

---

## Scenario File Format

Each scenario file defines a sequence of full order snapshots.

Example:

```json
{
  "scenarioName": "delayed-orders",
  "description": "Orders are created and accepted but not delivered or cancelled before expected delivery time.",
  "topic": "Orders",
  "topicType": "compacted",
  "keyField": "orderId",
  "defaultOrderCount": 100,
  "events": [
    {
      "step": 1,
      "delayAfterPreviousEventMs": 0,
      "messageKey": "{{orderId}}",
      "value": {
        "orderId": "{{orderId}}",
        "customerId": "{{customerId}}",
        "storeId": "{{storeId}}",
        "status": "CREATED",
        "expectedDeliveryTime": "now+60s",
        "createdAt": "now",
        "lastUpdatedAt": "now",
        "eventTime": "now",
        "stateLogs": [
          {
            "status": "CREATED",
            "at": "now"
          }
        ],
        "schemaVersion": 1
      }
    }
  ],
  "expectedResult": {
    "smsCommandExpected": true,
    "expectedSmsCommandCountPerOrder": 1
  }
}
```

---

## Dynamic Time Expressions

Scenario files may use dynamic time expressions.

Supported examples:

```text
now
now+500ms
now+10s
now+2m
now+1h
now+1d
now-10s
```

The simulator resolves these values to real UTC ISO-8601 timestamps before publishing to Kafka.

Example:

```json
{
  "expectedDeliveryTime": "now+60s"
}
```

May become:

```json
{
  "expectedDeliveryTime": "2026-05-12T19:30:00Z"
}
```

---

## Template Variables

Scenario files may use template variables.

Supported variables:

```text
{{orderId}}
{{customerId}}
{{storeId}}
```

Example:

```json
{
  "orderId": "{{orderId}}",
  "customerId": "{{customerId}}",
  "storeId": "{{storeId}}"
}
```

For each generated order, the simulator replaces these values with deterministic IDs.

Example generated IDs:

```text
orderId: ord-delayed-orders-0001
customerId: cus-0001
storeId: store-001
```

---

## Duplicate Events

Duplicate events are defined using `duplicateOfStep`.

Example:

```json
{
  "step": 2,
  "delayAfterPreviousEventMs": 500,
  "messageKey": "{{orderId}}",
  "duplicateOfStep": 1
}
```

This republishes the same full order state generated at step `1`.

---

## Mixed Scenario

The mixed scenario distributes generated orders across multiple scenario types.

Example:

```json
{
  "scenarioName": "mixed-orders",
  "defaultOrderCount": 100,
  "distribution": {
    "onTimePercentage": 35,
    "delayedPercentage": 25,
    "cancelledPercentage": 15,
    "etaUpdatedPercentage": 10,
    "duplicatePercentage": 10,
    "outOfOrderPercentage": 5
  },
  "includedScenarios": [
    "on-time-orders",
    "delayed-orders",
    "cancelled-orders",
    "eta-updated-orders",
    "duplicate-events",
    "out-of-order-updates"
  ]
}
```

Run:

```bash
PYTHONPATH=src python -m order_simulator.main \
  --scenario mixed-orders \
  --orders-count 100
```

---

## Failure Testing

The simulator supports failure testing through pause configuration.

Example:

```bash
SCENARIO=failure-recovery \
ORDERS_COUNT=100 \
PAUSE_AFTER_STEP=2 \
PAUSE_DURATION_SECONDS=60 \
PYTHONPATH=src python -m order_simulator.main
```

Suggested failure test flow:

```text
1. Start Kafka and Flink.
2. Start the Flink job.
3. Run the failure-recovery simulator scenario.
4. Wait until initial order states are consumed and timers are registered.
5. Stop or restart the Flink job before expectedDeliveryTime.
6. Restore the Flink job from checkpoint or savepoint.
7. Wait until expectedDeliveryTime passes.
8. Verify that only one SMS command is emitted per delayed order.
```

This validates:

- Flink state recovery
- Timer recovery
- Kafka offset recovery
- Duplicate prevention
- Idempotent SMS command generation

---

## Suggested Makefile Commands

From the repository root:

```bash
make simulate
make simulate-dry-run
make simulate-on-time
make simulate-delayed
make simulate-cancelled
make simulate-eta-updated
make simulate-duplicates
make simulate-out-of-order
make simulate-failure-recovery
make simulate-mixed
```

Example with custom order count:

```bash
ORDERS_COUNT=100 make simulate-mixed
```

Example dry-run:

```bash
ORDERS_COUNT=3 make simulate-dry-run
```

---

## Verifying Produced Events

Using Kafka UI:

```text
http://localhost:8080
```

Open topic:

```text
Orders
```

Or use Kafka CLI:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic Orders \
  --from-beginning \
  --property print.key=true
```

The key should be the order ID:

```text
ord-delayed-orders-0001
```

The value should be the full latest order state.

---

## Verifying Flink Output

The Flink job consumes from:

```text
Orders
```

And should emit delay SMS commands to:

```text
sms-commands
```

For delayed orders, expected output:

```json
{
  "commandId": "ord-delayed-orders-0001:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS",
  "orderId": "ord-delayed-orders-0001",
  "reason": "ORDER_DELAYED",
  "schemaVersion": 1
}
```

For on-time and cancelled orders:

```text
No SEND_DELAY_SMS command should be emitted.
```

---

## Development Guidelines

When adding a new scenario:

1. Add a new JSON file under `simulator/scenarios/`.
2. Use full order state snapshots.
3. Use `orderId` as the Kafka message key.
4. Use dynamic time expressions where possible.
5. Add expected results to the scenario file.
6. Update this README if the scenario introduces new behavior.
7. Validate the scenario with dry-run mode before publishing to Kafka.

---

## Important Rules

The simulator must follow these rules:

- Publish to the `Orders` topic.
- Use `orderId` as the Kafka message key.
- Publish full latest order state on every update.
- Do not publish partial update events.
- Keep generated data deterministic enough for repeatable tests.
- Support `ORDERS_COUNT`, defaulting to `100`.
- Support all required scenarios.
- Support failure and recovery testing.

---

## Acceptance Criteria

The simulator is complete when:

- It can generate a configurable number of orders.
- Default order count is `100`.
- It publishes to the compacted `Orders` topic.
- Kafka message key is `orderId`.
- Each Kafka message value contains the full latest order state.
- Every order update produces a new message.
- All required scenarios are supported.
- Mixed scenario can generate multiple scenario types in one run.
- Duplicate and out-of-order scenarios are supported.
- Failure and recovery testing flows are supported.
- Dry-run mode works without Kafka.
- Usage and configuration are documented.