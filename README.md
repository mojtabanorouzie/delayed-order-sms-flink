# Delayed Order SMS Flink

An Apache Flink streaming job that detects delayed orders in real time and emits idempotent SMS commands for customer care.

## Problem Statement

When customer orders pass their expected delivery time without being delivered or cancelled, we want to trigger a customer care SMS such as:

> "We are sorry for the delay. We are checking the issue with your order."

This project does **not** send SMS directly. It emits an idempotent command to a Kafka topic; a downstream notification service can consume that command and send the actual SMS.

## Goals

- Build a local end-to-end stream processing environment
- Simulate realistic order events
- Process order events using Apache Flink
- Detect orders that pass their expected delivery time
- Emit a `SEND_DELAY_SMS` command for delayed orders
- Avoid duplicate SMS commands
- Practice Flink: Kafka Source/Sink, Keyed State, Processing Time Timers, Checkpointing, Failure Recovery, Savepoints, Idempotency
- Prepare a production proposal

## Non-Goals

- Sending real SMS messages
- Connecting directly to external SMS providers from Flink
- Building a production-ready system in the first phase
- Handling all possible order lifecycle complexities from day one

## Architecture

```
Order Event Simulator
        |
        v
Kafka Topic: Orders (compacted, keyed by orderId)
        |
        v
Apache Flink Job
        |
        v
Kafka Topic: sms-commands
        |
        v
Notification Service / SMS Orchestrator  (not implemented in this POC)
```

**Processing Logic:**
1. Read order state events from Kafka topic `Orders`
2. Deserialize JSON into `OrderState` objects
3. Key by `orderId`
4. `KeyedProcessFunction` maintains state (status, expectedDeliveryTime, delaySmsEmitted) and registers processing-time timers
5. When timer fires and order is not delivered/cancelled, emit `SEND_DELAY_SMS` command to `sms-commands`
6. Invalid/malformed events are routed to `dead-letter-events`

## Project Structure

```
delayed-order-sms-flink/
├── README.md
├── .gitignore
├── docker-compose.yml
├── run_scenario.py
├── strip_jackson.py
│
├── docs/
│   ├── adr/
│   │   ├── 0001-processing-time-vs-event-time.md
│   │   └── 0002-sms-idempotency-strategy.md
│   ├── rfc/
│   │   └── delayed-order-detection-rfc.md
│   └── runbooks/
│       ├── local-runbook.md
│       ├── failure-test-runbook.md
│       └── production-runbook.md
│
├── schemas/
│   ├── order-events/
│   │   └── order-state.schema.json
│   └── sms-commands/
│       └── send-delay-sms-command.schema.json
│
├── simulator/
│   ├── README.md
│   ├── requirements.txt
│   ├── scenarios/
│   └── src/
│
├── flink-job/
│   ├── README.md
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/company/delayedordersms/
│       │   ├── DelayedOrderSmsJob.java
│       │   ├── model/
│       │   │   ├── OrderState.java
│       │   │   ├── OrderStatus.java
│       │   │   ├── OrderDelayState.java
│       │   │   ├── SmsCommand.java
│       │   │   └── DeadLetterEvent.java
│       │   ├── processor/
│       │   │   └── DelayedOrderProcessFunction.java
│       │   ├── serde/
│       │   │   ├── OrderStateDeserializationFunction.java
│       │   │   ├── OrderStateParser.java
│       │   │   └── DeadLetterEventSerializationSchema.java
│       │   └── config/
│       │       └── JobConfig.java
│       └── test/java/com/company/delayedordersms/
│           ├── config/
│           ├── processor/
│           └── serde/
│
├── e2e-tests/
│   ├── README.md
│   └── run_e2e.py
│
└── proposal/
    └── production-proposal.md
```

## Kafka Topics

| Topic | Type | Description |
|---|---|---|
| `Orders` | Compacted | Latest order state snapshots, keyed by `orderId` |
| `sms-commands` | Regular | Delay SMS commands emitted by Flink |
| `dead-letter-events` | Regular | Invalid, malformed, or unsupported events |

## Event Contracts

### Input: OrderState (on `Orders` topic)

```json
{
  "orderId": "ord-123",
  "customerId": "cus-456",
  "storeId": "store-789",
  "status": "ACCEPTED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T18:45:00Z",
  "lastUpdatedAt": "2026-05-12T18:45:02Z",
  "eventTime": "2026-05-12T18:45:00Z",
  "stateLogs": [
    { "status": "CREATED", "at": "2026-05-12T18:30:00Z" },
    { "status": "ACCEPTED", "at": "2026-05-12T18:45:00Z" }
  ],
  "schemaVersion": 1
}
```

### Output: SendDelaySmsCommand (on `sms-commands` topic)

```json
{
  "commandId": "ord-123:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS",
  "orderId": "ord-123",
  "customerId": "cus-456",
  "storeId": "store-789",
  "reason": "ORDER_DELAYED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T19:31:00Z",
  "schemaVersion": 1
}
```

The `commandId` is idempotent: `{orderId}:DELAY_SMS`. Downstream services deduplicate on this key.

## Time Semantics

This POC uses **processing time** timers. The business question is: *"Has the real current time passed the expected delivery time?"* Therefore, timers fire based on wall-clock time. Events still carry `eventTime` for observability and future event-time processing.

## Local Docker Environment

| Service | Description | URL / Port |
|---|---|---|
| Kafka | Local broker in KRaft mode | `localhost:9092` |
| Kafka UI | Topic browser | http://localhost:8080 |
| Flink JobManager | Cluster manager + REST API | http://localhost:8081 |
| Flink TaskManager | Worker | - |
| Kafka Init | Creates topics on startup | - |

From host: `localhost:9092`. From inside Docker network: `kafka:29092`.

## Running Locally

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ with `confluent-kafka` (`pip install -r simulator/requirements.txt`)
- Java 17+ and Maven 3.9+
- Ports `8080`, `8081`, `9092` available

### 1. Start Infrastructure

```bash
docker compose up -d
docker compose ps   # wait until all services are healthy/running
```

### 2. Build and Submit Flink Job

```bash
mvn clean package -f flink-job/pom.xml

docker exec flink-jobmanager flink run \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  /opt/flink/usrlib/delayed-order-sms-flink-job-0.1.0.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --sms.commands.topic sms-commands \
  --consumer.group.id delayed-order-sms-flink \
  --checkpoint.storage.path file:///opt/flink/checkpoints \
  --parallelism 1
```

Verify:
```bash
docker exec flink-jobmanager flink list 2>&1
# Expected: Delayed Order SMS Detection Job (RUNNING)
```

### 3. Run Simulator Scenarios

```bash
python run_scenario.py delayed-orders     --orders-count 5
python run_scenario.py on-time-orders     --orders-count 5
python run_scenario.py cancelled-orders    --orders-count 5
python run_scenario.py duplicate-events    --orders-count 5
python run_scenario.py eta-updated-orders  --orders-count 5
python run_scenario.py mixed-orders        --orders-count 5
```

### 4. Verify Output

```bash
# Consume SMS commands
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic sms-commands --from-beginning \
  --max-messages 50 --timeout-ms 20000

# Check dead letter queue
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dead-letter-events --from-beginning \
  --max-messages 10 --timeout-ms 5000
```

**UIs:** Flink at http://localhost:8081, Kafka at http://localhost:8080.

### 5. Tear Down

```bash
docker compose down         # keep data
docker compose down -v      # remove all data
```

## Verified Test Results

| Scenario | Orders | SMS | Key Behavior |
|---|---|---|---|
| delayed-orders | 5 | 5 | One SMS per order after expectedDeliveryTime |
| on-time-orders | 5 | 0 | Delivered before deadline, no SMS |
| cancelled-orders | 5 | 0 | Cancelled before deadline, no SMS |
| duplicate-events | 5 (10 events) | 5 | Exactly-once: duplicates produce only 1 SMS each |
| eta-updated-orders | 5 | 5 | Timer uses updated expectedDeliveryTime |
| mixed-orders | 5 | varies (~1-2) | Only delayed-type orders emit SMS |

### Verified Behaviors

- **Exactly-once deduplication**: duplicate events produce only 1 SMS per order
- **Idempotency**: each `commandId = orderId + ":DELAY_SMS"` appears at most once
- **On-time suppression**: orders reaching DELIVERED before deadline produce 0 SMS
- **Cancellation suppression**: orders reaching CANCELLED before deadline produce 0 SMS
- **ETA updates**: job uses the latest `expectedDeliveryTime` for timer evaluation
- **Fault tolerance**: checkpoints every 10s; transient Kafka errors self-resolve
- **Dead-letter queue**: remains empty for all tested scenarios

## E2E Tests

```bash
python e2e-tests/run_e2e.py            # run all 6 scenarios
python e2e-tests/run_e2e.py --no-cleanup  # leave infrastructure running
```

## Common Issues

| Symptom | Cause | Solution |
|---|---|---|
| `CoordinatorNotAvailableException` in TaskManager logs | Kafka not yet ready | Self-resolves |
| `kafka-init` exited | Topics already exist | Harmless; verify with `kafka-topics --list` |
| SMS not appearing immediately | Timer uses processing time | Wait for `expectedDeliveryTime` to pass |
| `TimeoutException` from consumer | No new messages | Normal; check `Processed a total of N messages` |

## Design Decisions

See [docs/adr/](docs/adr/) for Architecture Decision Records:
- [ADR-0001](docs/adr/0001-processing-time-vs-event-time.md): Processing time vs event time
- [ADR-0002](docs/adr/0002-sms-idempotency-strategy.md): SMS idempotency strategy

See [docs/rfc/](docs/rfc/) for the full architecture RFC.
See [docs/runbooks/](docs/runbooks/) for operational runbooks.
See [proposal/](proposal/) for the production deployment proposal.

## License

Internal use only.