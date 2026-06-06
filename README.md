# Delayed Order SMS — Flink Showcase

[![Java 17](https://img.shields.io/badge/java-17-orange.svg)](https://adoptium.net/)
[![Apache Flink 1.19](https://img.shields.io/badge/flink-1.19.1-blue.svg)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/kafka-KRaft-black.svg)](https://kafka.apache.org/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://docs.docker.com/compose/)

A production-ready Apache Flink showcase demonstrating five independent streaming jobs across three complexity tiers: stateful timers, windowed aggregation, and dual-stream joins with external data enrichment.

## Jobs Overview

| # | Job | Pattern | Output Topic |
|---|-----|---------|--------------|
| 1 | **Delayed Order SMS** | `KeyedProcessFunction` + processing-time timer | `sms-commands` |
| 2 | **Auto-Refund** | `KeyedProcessFunction` + delayed timer | `refund-commands` |
| 3 | **Courier Overload Detection** | `KeyedProcessFunction` + `MapState` | `courier-pause-commands` |
| 4 | **Restaurant Queue Bottleneck** | `KeyedProcessFunction` + tumbling window | `restaurant-alerts` |
| 5 | **Dynamic Surge Pricing** | `KeyedCoProcessFunction` (orders × weather) | `surge-pricing-signals` |

All jobs share: RocksDB state backend, exactly-once checkpointing, dead-letter routing via side outputs, idempotent output keys, and per-job consumer groups.

## Architecture

```text
                          ┌─────────────────────────────────────────┐
  Order Event Simulator   │            Apache Flink Cluster          │
        │                 │                                           │
        ▼                 │  ┌─────────────────────────────────────┐ │
  ┌──────────────┐        │  │  Job 1: DelayedOrderSmsJob          │ │──▶ sms-commands
  │ Kafka:Orders │──────▶ │  │  keyBy(orderId) · timer at ETA      │ │
  │  (compacted) │        │  └─────────────────────────────────────┘ │
  └──────────────┘        │  ┌─────────────────────────────────────┐ │
        │                 │  │  Job 2: AutoRefundJob               │ │──▶ refund-commands
        ├────────────────▶│  │  keyBy(orderId) · delayed timer     │ │
        │                 │  └─────────────────────────────────────┘ │
        │                 │  ┌─────────────────────────────────────┐ │
        ├────────────────▶│  │  Job 3: CourierOverloadJob          │ │──▶ courier-pause-commands
        │                 │  │  keyBy(courierId) · MapState count  │ │
        │                 │  └─────────────────────────────────────┘ │
        │                 │  ┌─────────────────────────────────────┐ │
        ├────────────────▶│  │  Job 4: RestaurantBottleneckJob     │ │──▶ restaurant-alerts
        │                 │  │  keyBy(storeId) · 5-min window      │ │
        │                 │  └─────────────────────────────────────┘ │
        │                 │  ┌─────────────────────────────────────┐ │
        └────────────────▶│  │  Job 5: SurgePricingJob             │ │──▶ surge-pricing-signals
                          │  │  keyBy(zoneId) · CoProcessFunction  │ │
  Kafka:weather-data ────▶│  │  (orders ⋈ weather)                 │ │
                          │  └─────────────────────────────────────┘ │
                          │                                           │
                          │  All jobs ──▶ dead-letter-events          │
                          └─────────────────────────────────────────-─┘
```

## Kafka Topics

| Topic | Partitions | Type | Purpose |
|-------|-----------|------|---------|
| `Orders` | 3 | Compacted | Full order-state snapshots keyed by `orderId` |
| `weather-data` | 4 | Compacted | Weather per zone, keyed by `region` |
| `sms-commands` | 3 | Regular | Delay SMS commands (Job 1) |
| `refund-commands` | 8 | Regular | Auto-refund commands (Job 2) |
| `courier-pause-commands` | 8 | Regular | Courier pause/resume commands (Job 3) |
| `restaurant-alerts` | 4 | Regular | Restaurant bottleneck alerts (Job 4) |
| `surge-pricing-signals` | 8 | Regular | Dynamic surge pricing signals (Job 5) |
| `dead-letter-events` | 3 | Regular | Malformed or invalid events from all jobs |

## Quick Start — Automated E2E

```bash
# 1. Run everything (builds JAR, starts infra, submits all 5 jobs, validates)
python e2e-tests/run_e2e.py

# Leave infra running after tests
python e2e-tests/run_e2e.py --no-cleanup
```

The e2e run takes ~6 minutes and validates all 5 jobs across 8 scenarios. See [e2e-tests/README.md](e2e-tests/README.md) for expected results.

## Manual Setup

### Prerequisites

| Tool | Version |
|------|---------|
| Docker + Compose | 24.0+ / v2 plugin |
| Java JDK | 17 |
| Maven | 3.9+ |
| Python | 3.9+ |

```bash
pip install -r simulator/requirements.txt
```

Ports required: `8080` (Kafka UI), `8081` (Flink UI), `9092` (Kafka).

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps   # wait until all healthy
```

### 2. Build the fat JAR

```bash
mvn clean package -f flink-job/pom.xml -DskipTests
```

### 3. Submit all five jobs

```bash
# Copy JAR into the jobmanager container
docker cp flink-job/target/delayed-order-sms-flink-job.jar \
  flink-jobmanager:/tmp/delayed-order-sms-flink-job.jar

JAR=/tmp/delayed-order-sms-flink-job.jar
BASE="--kafka.bootstrap.servers kafka:29092 --orders.topic Orders \
      --checkpoint.storage.path file:///opt/flink/checkpoints --parallelism 1"

# Job 1 — Delayed Order SMS
docker exec flink-jobmanager flink run -d -c com.company.delayedordersms.DelayedOrderSmsJob \
  $JAR $BASE --sms.commands.topic sms-commands --consumer.group.id delayed-order-sms-flink

# Job 2 — Auto-Refund
docker exec flink-jobmanager flink run -d -c com.company.delayedordersms.AutoRefundJob \
  $JAR $BASE --refund.commands.topic refund-commands --consumer.group.id auto-refund-flink \
  --refund.delay.minutes 30

# Job 3 — Courier Overload
docker exec flink-jobmanager flink run -d -c com.company.delayedordersms.CourierOverloadJob \
  $JAR $BASE --courier.commands.topic courier-pause-commands \
  --consumer.group.id courier-overload-flink --overload.threshold 8 --resume.threshold 5

# Job 4 — Restaurant Bottleneck
docker exec flink-jobmanager flink run -d -c com.company.delayedordersms.RestaurantBottleneckJob \
  $JAR $BASE --restaurant.alerts.topic restaurant-alerts \
  --consumer.group.id restaurant-bottleneck-flink --window.size.seconds 300

# Job 5 — Dynamic Surge Pricing
docker exec flink-jobmanager flink run -d -c com.company.delayedordersms.SurgePricingJob \
  $JAR $BASE --surge.signals.topic surge-pricing-signals \
  --consumer.group.id surge-pricing-flink --window.size.seconds 60

docker exec flink-jobmanager flink list   # expect 5 RUNNING jobs
```

### 4. Run scenarios

```bash
# Individual scenarios (from project root)
docker compose --profile e2e run --rm simulator \
  --scenario delayed-orders --orders-count 5 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders

# Monitor output
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic sms-commands --from-beginning --timeout-ms 10000
```

### 5. Tear down

```bash
docker compose down -v
```

## Project Structure

```text
delayed-order-sms-flink/
├── README.md
├── docker-compose.yml
├── docs/
│   ├── architecture.md              ← System design and data flows
│   ├── adr/                         ← Architecture Decision Records
│   │   ├── 0001-processing-time-vs-event-time.md
│   │   ├── 0002-sms-idempotency-strategy.md
│   │   ├── 0003-separate-jobs-per-feature.md
│   │   ├── 0004-weather-coprocess-function.md
│   │   └── 0005-rocksdb-state-backend.md
│   └── runbooks/
│       ├── local-runbook.md         ← Local dev setup and troubleshooting
│       ├── failure-test-runbook.md  ← Checkpoint/savepoint recovery
│       ├── production-runbook.md    ← Production ops
│       ├── dead-letter-runbook.md   ← DLQ investigation and replay
│       ├── surge-pricing-runbook.md ← Surge pricing tuning
│       └── scaling-runbook.md       ← Parallelism and capacity scaling
│
├── flink-job/                       ← All 5 Flink jobs (single Maven module)
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/company/delayedordersms/
│       │   ├── DelayedOrderSmsJob.java
│       │   ├── AutoRefundJob.java
│       │   ├── CourierOverloadJob.java
│       │   ├── RestaurantBottleneckJob.java
│       │   ├── SurgePricingJob.java
│       │   ├── config/              ← One *JobConfig record per job
│       │   ├── model/               ← OrderState, WeatherData, output models
│       │   ├── processor/           ← One ProcessFunction per job
│       │   └── serde/               ← Serialization schemas
│       └── test/                    ← 100 unit tests across all processors
│
├── simulator/                       ← Python order/weather event generator
│   └── scenarios/                   ← 10 JSON scenario files
│
└── e2e-tests/
    └── run_e2e.py                   ← Automated end-to-end test runner
```

## Test Coverage

```bash
mvn test -f flink-job/pom.xml
# Tests run: 100, Failures: 0, Errors: 0
```

| Processor | Test Class | Tests |
|-----------|-----------|-------|
| `DelayedOrderProcessFunction` | `DelayedOrderProcessFunctionTest` | 10 |
| `DelayedOrderRefundProcessFunction` | `DelayedOrderRefundProcessFunctionTest` | 20 |
| `CourierOverloadProcessFunction` | `CourierOverloadProcessFunctionTest` | 21 |
| `RestaurantBottleneckProcessFunction` | `RestaurantBottleneckProcessFunctionTest` | 18 |
| `DynamicSurgePricingCoProcessFunction` | `DynamicSurgePricingCoProcessFunctionTest` | 25 |
| Config + Serde | `JobConfigTest`, `OrderStateDeserializationFunctionTest` | 6 |

## Design Decisions

| ADR | Decision |
|-----|----------|
| [ADR-0001](docs/adr/0001-processing-time-vs-event-time.md) | Use processing-time timers — business deadlines are wall-clock |
| [ADR-0002](docs/adr/0002-sms-idempotency-strategy.md) | Idempotency via deterministic `commandId` + boolean flag in state |
| [ADR-0003](docs/adr/0003-separate-jobs-per-feature.md) | One Flink job per feature — independent deployment, failure isolation |
| [ADR-0004](docs/adr/0004-weather-coprocess-function.md) | `KeyedCoProcessFunction` for weather enrichment — avoids blocking I/O |
| [ADR-0005](docs/adr/0005-rocksdb-state-backend.md) | RocksDB — incremental checkpoints, unbounded state growth |

## Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `CoordinatorNotAvailableException` | Kafka not yet ready | Transient — retries succeed automatically |
| `kafka-init` exited 0 | Topics already exist | Harmless — verify with `kafka-topics --list` |
| SMS/refund not appearing | Timer hasn't fired | Check `expectedDeliveryTime` value in scenario |
| No surge signals | Weather not injected yet | Weather events must arrive before orders in the window |
| Job shows `RESTARTING` | Kafka broker unavailable | Check `docker compose ps` and wait for Kafka to be healthy |

See [docs/runbooks/](docs/runbooks/) for detailed operational procedures.

## Local Services

| Service | URL | Notes |
|---------|-----|-------|
| Flink UI | [localhost:8081](http://localhost:8081) | Job graph, metrics, checkpoints |
| Kafka UI | [localhost:8080](http://localhost:8080) | Topic browser, message inspector |
| Kafka | `localhost:9092` | Direct producer/consumer access |
