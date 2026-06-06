# End-to-End Tests

Automated test runner that builds the fat JAR, starts the full Docker stack, submits all five Flink jobs, runs scenario simulations, and validates the output on each Kafka topic.

---

## Quick Start

```bash
# Full run (builds, starts infra, runs all scenarios, tears down)
python e2e-tests/run_e2e.py

# Leave infrastructure running after tests complete
python e2e-tests/run_e2e.py --no-cleanup
```

Expected runtime: ~6 minutes. Expected outcome: all 14 scenarios pass.

---

## Prerequisites

- Docker and Docker Compose v2
- Python 3.9+ with `confluent-kafka` (`pip install -r simulator/requirements.txt`)
- Java 17+ and Maven 3.9+
- Ports `8080`, `8081`, `9092` free on localhost

---

## What the runner does

1. **Phase 1** — Tears down any previous stack, starts fresh infrastructure (Kafka + Flink), waits for healthy status, creates all 8 Kafka topics, fixes checkpoint directory permissions.
2. **Phase 2** — Builds the fat JAR (`mvn clean package`), copies it into the `flink-jobmanager` container, submits all five jobs with e2e-appropriate parameters (e.g., `--window.size.seconds 10` for fast timer tests).
3. **Phase 3** — SMS scenarios: delayed, on-time, cancelled, duplicate, ETA-updated, mixed.
4. **Phase 4** — Auto-refund scenario: severely-delayed-refund.
5. **Phase 5** — Courier overload scenario: 10 orders to the same courier.
6. **Phase 6** — Restaurant bottleneck scenario: 5 orders with 20-min pickup delta.
7. **Phase 7** — Surge pricing scenario: injects RAIN weather, then 5 at-risk orders.
8. **Phase 8** — Prints results table.
9. **Phase 9** — Tears down (skipped with `--no-cleanup`).

---

## Expected Results

### SMS (Job 1)

| Scenario | Orders | Expected SMS | Behaviour |
|---|---|---|---|
| `delayed-orders` | 5 | 5 | ETA in the past — timer fires immediately |
| `on-time-orders` | 5 | 0 | Delivered before ETA — timer cancelled |
| `cancelled-orders` | 5 | 0 | Cancelled before ETA — timer cancelled |
| `duplicate-events` | 5 (10 events) | 5 | `delaySmsEmitted` flag prevents duplicates |
| `eta-updated-orders` | 5 | 5 | Timer tracks updated ETA, not original |
| `mixed-orders` | 5 | 0–2 | ~25% delayed-type orders in the mix |

### Auto-Refund (Job 2)

| Scenario | Orders | Expected refunds | Behaviour |
|---|---|---|---|
| `severely-delayed-refund` | 5 | 5 | ETA + refundDelay=0min → fires immediately |

### Courier Overload (Job 3)

| Scenario | Expected PAUSE commands | Behaviour |
|---|---|---|
| `courier-overload` | 1 | 10 orders to same courier; PAUSE at 8th active order |

### Restaurant Bottleneck (Job 4)

| Scenario | Expected alerts | Behaviour |
|---|---|---|
| `restaurant-bottleneck` | 1–5 | 20-min avg pickup > 15-min threshold → CRITICAL in 10-sec window |

### Surge Pricing (Job 5)

| Scenario | Expected signals | Behaviour |
|---|---|---|
| `surge-pricing` | 1–3 | RAIN weather + 5 at-risk orders → multiplier 1.6 > threshold 1.15 |

---

## Submitting Jobs Manually

If you want to submit jobs individually against a running cluster:

```bash
# Copy JAR first
docker cp flink-job/target/delayed-order-sms-flink-job.jar \
  flink-jobmanager:/tmp/delayed-order-sms-flink-job.jar

JAR=/tmp/delayed-order-sms-flink-job.jar
BASE="--kafka.bootstrap.servers kafka:29092 --orders.topic Orders \
      --checkpoint.storage.path file:///opt/flink/checkpoints --parallelism 1"

# Job 1
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.DelayedOrderSmsJob $JAR $BASE \
  --sms.commands.topic sms-commands --consumer.group.id delayed-order-sms-flink

# Job 2
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.AutoRefundJob $JAR $BASE \
  --refund.commands.topic refund-commands --consumer.group.id auto-refund-flink \
  --refund.delay.minutes 0

# Job 3
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.CourierOverloadJob $JAR $BASE \
  --courier.commands.topic courier-pause-commands \
  --consumer.group.id courier-overload-flink \
  --overload.threshold 8 --resume.threshold 5

# Job 4
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.RestaurantBottleneckJob $JAR $BASE \
  --restaurant.alerts.topic restaurant-alerts \
  --consumer.group.id restaurant-bottleneck-flink \
  --window.size.seconds 10

# Job 5
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.SurgePricingJob $JAR $BASE \
  --surge.signals.topic surge-pricing-signals \
  --consumer.group.id surge-pricing-flink \
  --window.size.seconds 10 --surge.threshold 1.15

docker exec flink-jobmanager flink list   # expect 5 RUNNING
```

---

## Running Scenarios Manually

```bash
# Any SMS scenario
docker compose --profile e2e run --rm simulator \
  --scenario delayed-orders --orders-count 5 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders

# Inject weather for surge pricing (BEFORE running surge-pricing scenario)
docker exec kafka bash -c \
  "echo '{\"region\":\"zone-surge-e2e\",\"condition\":\"RAIN\",\"temperature\":15.0,\"windSpeed\":10.0,\"schemaVersion\":1}' \
   | kafka-console-producer --bootstrap-server kafka:29092 --topic weather-data"

# Surge pricing scenario
docker compose --profile e2e run --rm simulator \
  --scenario surge-pricing --orders-count 5 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders
```

---

## Consuming Output Topics

```bash
# SMS commands
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic sms-commands \
  --from-beginning --max-messages 20 --timeout-ms 10000

# Surge pricing signals
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic surge-pricing-signals \
  --from-beginning --max-messages 10 --timeout-ms 15000

# Dead-letter queue (should be empty in healthy runs)
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic dead-letter-events \
  --from-beginning --max-messages 10 --timeout-ms 5000
```

---

## Common Issues

**Surge signals not appearing**
The SurgePricingJob uses `OffsetsInitializer.latest()` on the weather topic. Inject the weather event *after* the job is running and *before* the order scenario. The e2e runner does this automatically (2-second pause between injection and scenario).

**Restaurant bottleneck: timer fires but no alert**
The `--window.size.seconds 10` flag must be set. Default is 300 seconds, which would never fire in the e2e window.

**`kafka-init` exited with error**
Expected if topics already exist. Verify with `docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list`. If all 8 topics are present, this is harmless.

**Job in RESTARTING state**
Check `docker compose ps` — Kafka or Flink may be starting up. The fixed-delay restart strategy (3 attempts, 5s delay) handles transient failures automatically.

---

## Failure Recovery Test

See [docs/runbooks/failure-test-runbook.md](../docs/runbooks/failure-test-runbook.md).

1. Submit jobs with `--no-cleanup` flag
2. Run `failure-recovery` scenario
3. Cancel the job via Flink UI
4. Re-submit the same job — it restores from the latest checkpoint
5. Verify: exactly-once output (no duplicates, no missing records)
