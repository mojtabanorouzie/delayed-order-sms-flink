# Delayed Order SMS — Flink

[![Java 17+](https://img.shields.io/badge/java-17+-orange.svg)](https://adoptium.net/)
[![Apache Flink 1.19](https://img.shields.io/badge/flink-1.19-blue.svg)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/kafka-KRaft-black.svg)](https://kafka.apache.org/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://docs.docker.com/compose/)

An Apache Flink streaming job that detects delayed food/delivery orders in real time and emits idempotent SMS commands for customer care.

## Problem Statement

When an order passes its expected delivery time without being delivered or cancelled, we want to trigger a customer care SMS:

> "We are sorry for the delay. We are checking the issue with your order."

This project does **not** send SMS directly. It emits an idempotent `SEND_DELAY_SMS` command to a Kafka topic; a downstream notification service consumes that command and sends the actual SMS.

## Architecture

```text
Order Event Simulator
        │
        ▼
Kafka Topic: Orders  (compacted, keyed by orderId)
        │
        ▼
Apache Flink Job  (KeyedProcessFunction + processing-time timers)
        │
        ├──▶ Kafka Topic: sms-commands
        └──▶ Kafka Topic: dead-letter-events
```

**Processing logic:**

1. Read order state snapshots from the `Orders` topic
2. Deserialize JSON → `OrderState`; route malformed messages to `dead-letter-events`
3. Key by `orderId`
4. `KeyedProcessFunction` maintains per-order state and registers a processing-time timer at `expectedDeliveryTime`
5. On timer fire: if the order is still active (not delivered/cancelled), emit one `SEND_DELAY_SMS` command
6. Stale out-of-order updates (older `lastUpdatedAt`) are ignored; the `delaySmsEmitted` flag prevents duplicate commands

## Project Structure

```text
delayed-order-sms-flink/
├── README.md
├── .gitignore
├── docker-compose.yml
├── run_scenario.py            # manual scenario runner
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
│   ├── order-events/order-state.schema.json
│   └── sms-commands/send-delay-sms-command.schema.json
│
├── simulator/
│   ├── README.md
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── scenarios/
│   └── src/
│
├── flink-job/
│   ├── README.md
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/company/delayedordersms/
│       │   ├── DelayedOrderSmsJob.java
│       │   ├── config/JobConfig.java
│       │   ├── model/
│       │   ├── processor/DelayedOrderProcessFunction.java
│       │   └── serde/
│       └── test/java/com/company/delayedordersms/
│
├── e2e-tests/
│   ├── README.md
│   ├── run_e2e.py
│   └── scenarios/
│
└── proposal/
    └── production-proposal.md
```

## Kafka Topics

| Topic | Type | Description |
| --- | --- | --- |
| `Orders` | Compacted | Latest order state snapshots, keyed by `orderId` |
| `sms-commands` | Regular | Delay SMS commands emitted by Flink |
| `dead-letter-events` | Regular | Malformed or invalid events |

## Event Contracts

### Input — OrderState (`Orders` topic)

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
    { "status": "CREATED",  "at": "2026-05-12T18:30:00Z" },
    { "status": "ACCEPTED", "at": "2026-05-12T18:45:00Z" }
  ],
  "schemaVersion": 1
}
```

### Output — SendDelaySmsCommand (`sms-commands` topic)

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

`commandId = {orderId}:DELAY_SMS` is the idempotency key — downstream services deduplicate on it.

## Time Semantics

Processing-time timers are used intentionally. The business question is *"has wall-clock time passed the expected delivery time?"*, not replay fidelity. Events still carry `eventTime` for observability and future event-time migration.

See [ADR-0001](docs/adr/0001-processing-time-vs-event-time.md).

## Local Docker Environment

| Service | Description | URL / Port |
| --- | --- | --- |
| Kafka | KRaft-mode broker | `localhost:9092` |
| Kafka UI | Topic browser | [localhost:8080](http://localhost:8080) |
| Flink JobManager | Cluster manager + REST API | [localhost:8081](http://localhost:8081) |
| Flink TaskManager | Worker | — |

## Running Locally

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ (`pip install -r simulator/requirements.txt`)
- Java 17+ and Maven 3.9+
- Ports `8080`, `8081`, `9092` available

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps   # wait until all services are healthy
```

### 2. Build and submit the Flink job

```bash
mvn clean package -f flink-job/pom.xml

docker exec flink-jobmanager flink run \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  /opt/flink/usrlib/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --sms.commands.topic sms-commands \
  --dead.letter.topic dead-letter-events \
  --consumer.group.id delayed-order-sms-flink \
  --checkpoint.storage.path file:///opt/flink/checkpoints \
  --parallelism 1
```

Verify:

```bash
docker exec flink-jobmanager flink list
# Expected: Delayed Order SMS Detection Job  (RUNNING)
```

### 3. Run scenarios manually

```bash
python run_scenario.py delayed-orders      --orders-count 5
python run_scenario.py on-time-orders      --orders-count 5
python run_scenario.py cancelled-orders    --orders-count 5
python run_scenario.py duplicate-events    --orders-count 5
python run_scenario.py eta-updated-orders  --orders-count 5
python run_scenario.py mixed-orders        --orders-count 5
```

### 4. Verify output

```bash
# SMS commands
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic sms-commands --from-beginning \
  --max-messages 50 --timeout-ms 20000

# Dead-letter queue
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dead-letter-events --from-beginning \
  --max-messages 10 --timeout-ms 5000
```

**UIs:** Flink → [http://localhost:8081](http://localhost:8081) · Kafka → [http://localhost:8080](http://localhost:8080)

### 5. Tear down

```bash
docker compose down       # keep volumes
docker compose down -v    # remove all data
```

## E2E Tests

```bash
python e2e-tests/run_e2e.py             # run all scenarios, clean up after
python e2e-tests/run_e2e.py --no-cleanup  # leave infrastructure running
```

See [e2e-tests/README.md](e2e-tests/README.md) for details.

## Verified Test Results

| Scenario | Orders | SMS | Behaviour |
| --- | --- | --- | --- |
| delayed-orders | 5 | 5 | One SMS per order after `expectedDeliveryTime` |
| on-time-orders | 5 | 0 | Delivered before deadline — no SMS |
| cancelled-orders | 5 | 0 | Cancelled before deadline — no SMS |
| duplicate-events | 5 (10 events) | 5 | Exactly-once: duplicates → 1 SMS each |
| eta-updated-orders | 5 | 5 | Timer tracks the updated `expectedDeliveryTime` |
| mixed-orders | 5 | 0–2 | Only delayed orders emit SMS (25% rate over 5 orders) |

## Common Issues

| Symptom | Cause | Fix |
| --- | --- | --- |
| `CoordinatorNotAvailableException` in TaskManager | Kafka not yet ready | Self-resolves |
| `kafka-init` exited with 0 | Topics already exist | Harmless — verify with `kafka-topics --list` |
| SMS not appearing immediately | Processing-time timer | Wait for `expectedDeliveryTime` to pass |
| `TimeoutException` from consumer | No new messages | Normal — check `Processed a total of N messages` |

## Design Decisions

- [ADR-0001](docs/adr/0001-processing-time-vs-event-time.md) — Processing time vs event time
- [ADR-0002](docs/adr/0002-sms-idempotency-strategy.md) — SMS idempotency strategy
- [RFC](docs/rfc/delayed-order-detection-rfc.md) — Full architecture RFC
- [Runbooks](docs/runbooks/) — Operational runbooks
- [Production proposal](proposal/production-proposal.md)

## License

Internal use only.
