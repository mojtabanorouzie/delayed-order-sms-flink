# Order Event Simulator

The simulator is responsible for generating realistic order update events and publishing them to Kafka for local and end-to-end testing of the delayed order detection pipeline.

It simulates the behavior of the company order stream where each order update produces a new event containing the latest known state of the order.

---

## Purpose

The simulator is used to validate the Flink pipeline against controlled and repeatable order scenarios.

It should support:

- Producing a configurable number of orders.
- Publishing order updates to Kafka.
- Simulating all required business scenarios.
- Testing failure and recovery behavior.
- Supporting local development and workshop exercises.

---

## Source Topic Model

The simulator publishes events to the Kafka topic:

```
Orders
```

The `Orders` topic is a **compacted topic**.

Each Kafka message should be keyed by:

```text
orderId
```

Each message value should contain the **latest state of the order** at the time of the update.

This means that whenever an order changes, the simulator must publish a new event with the updated order state.

Example:

```text
Order created       -> publish latest order state
Order accepted      -> publish latest order state
Order picked up     -> publish latest order state
Order delivered     -> publish latest order state
Order cancelled     -> publish latest order state
ETA updated         -> publish latest order state
```

The Flink job consumes this topic and detects delayed orders based on the latest order state.

---

## Event Production Rule

For every order update, the simulator must emit one event.

The emitted event should represent the complete latest state of the order, not only the changed fields.

Example flow:

```text
ORDER_CREATED
ORDER_ACCEPTED
ORDER_PICKED_UP
ORDER_DELIVERED
```

The simulator should publish one event for each state change.

---

## Configuration

The simulator must be configurable.

Minimum required configuration:

| Config | Description | Default |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `ORDERS_TOPIC` | Target Kafka topic | `Orders` |
| `ORDERS_COUNT` | Number of orders to generate | `100` |
| `SCENARIO` | Scenario name to execute | `mixed` |
| `EVENT_DELAY_MS` | Delay between generated events | `500` |
| `ORDER_ID_PREFIX` | Prefix for generated order IDs | `ord` |

Example:

```bash
ORDERS_COUNT=100 \
SCENARIO=mixed \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
ORDERS_TOPIC=Orders \
./run-simulator.sh
```

---

## Required Scenarios

The simulator must support the following scenarios.

### 1. On-Time Orders

Orders are created and delivered before their expected delivery time.

Expected result:

```text
No delay SMS command should be emitted.
```

---

### 2. Delayed Orders

Orders are created but are not delivered or cancelled before their expected delivery time.

Expected result:

```text
One delay SMS command should be emitted per delayed order.
```

---

### 3. Cancelled Orders

Orders are created and cancelled before their expected delivery time.

Expected result:

```text
No delay SMS command should be emitted.
```

---

### 4. ETA Updated Orders

Orders receive an updated expected delivery time.

Expected result:

```text
The Flink job should use the latest expected delivery time.
```

---

### 5. Duplicate Events

The same order state is published more than once.

Expected result:

```text
Duplicate events must not produce duplicate SMS commands.
```

---

### 6. Out-of-Order Updates

Order updates are published in an unexpected order.

Expected result:

```text
The pipeline should not crash.
Behavior must follow the documented business rules.
```

---

### 7. Mixed Scenario

The simulator generates a mix of all supported scenarios across a configurable number of orders.

Example:

```text
ORDERS_COUNT=100
SCENARIO=mixed
```

Expected result:

```text
The pipeline should correctly process all order types in one run.
```

---

## Failure Testing Capabilities

The simulator should support failure and recovery testing.

Required capabilities:

- Generate orders with future expected delivery times.
- Pause after producing initial order states.
- Resume producing updates after a delay.
- Re-publish duplicate events after restart.
- Replay a scenario from the beginning.
- Produce events slowly enough to allow manual Flink failure testing.

These capabilities are required to test:

- Flink checkpoint recovery.
- Timer recovery.
- Duplicate prevention.
- Kafka offset recovery.
- Idempotent SMS command generation.

Example failure test flow:

```text
1. Start Kafka and Flink.
2. Start the simulator with delayed orders.
3. Produce orders with expectedDeliveryTime = now + 2 minutes.
4. Stop or restart the Flink job before the timer fires.
5. Restart the Flink job.
6. Wait until expectedDeliveryTime passes.
7. Verify that only one SMS command is emitted per delayed order.
```

---

## Expected Event Structure

Each event should contain the latest order state.

Example:

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "storeId": "store-001",
  "status": "ACCEPTED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "lastUpdatedAt": "2026-05-12T18:45:02Z",
  "eventTime": "2026-05-12T18:45:00Z",
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
| `expectedDeliveryTime` | Current expected delivery time |
| `lastUpdatedAt` | Time of latest update |
| `eventTime` | Business event time |
| `schemaVersion` | Schema version |

---

## Running the Simulator

From the project root:

```bash
make simulate
```

Run with custom order count:

```bash
ORDERS_COUNT=100 make simulate
```

Run a specific scenario:

```bash
SCENARIO=delayed ORDERS_COUNT=100 make simulate
```

Suggested scenario commands:

```bash
make simulate-on-time
make simulate-delayed
make simulate-cancelled
make simulate-eta-updated
make simulate-duplicates
make simulate-out-of-order
make simulate-mixed
```

---

## Verification

After running the simulator, verify that events are published to:

```text
Orders
```

The Flink job should consume from `Orders` and emit delay SMS commands only when required.

Output should be verified in:

```text
sms-commands
```

---

## Acceptance Criteria

The simulator is complete when:

- It can generate a configurable number of orders, defaulting to `100`.
- It publishes events to the compacted `Orders` topic.
- Kafka message key is `orderId`.
- Each event contains the latest full order state.
- Every order update produces a new event.
- All required scenarios are supported.
- Mixed scenario can generate multiple scenario types in one run.
- Failure and recovery testing flows are supported.
- Generated events are deterministic enough for repeatable tests.
- Usage and configuration are documented.
```