# End-to-End Tests

This document provides a practical guide for running end-to-end scenario tests against the delayed order detection pipeline, based on actual runtime experience.

---

## Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ with `confluent-kafka` (install: `pip install -r simulator/requirements.txt`)
- Java 11+ and Maven (for building the Flink job)
- Ports `8080`, `8081`, `9092` available on localhost

---

## Quick Start: Full E2E Test Run

### 1. Start Infrastructure

```bash
docker compose up -d
```

Wait ~30-60 seconds for all containers to become healthy:

```bash
docker compose ps
```

All five services should show `healthy` or `running`:
- `kafka`
- `kafka-ui`
- `flink-jobmanager`
- `flink-taskmanager`
- `kafka-init` (may have exited 0 — that's OK)

If `kafka-init` exited with error because topics already exist, that is harmless.

### 2. Build and Submit the Flink Job

```bash
cd flink-job
mvn clean package -q
cd ..
```

Submit the JAR to the Flink cluster:

```bash
docker exec flink-jobmanager flink run \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  /opt/flink/usrlib/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --sms.commands.topic sms-commands \
  --consumer.group.id delayed-order-sms-flink \
  --checkpoint.storage.path file:///opt/flink/checkpoints \
  --parallelism 1
```

Verify the job is running:

```bash
docker exec flink-jobmanager flink list 2>&1
```

Expected output:
```
Delayed Order SMS Detection Job (RUNNING)
```

Flink will auto-create the `sms-commands` and `dead-letter-events` topics on first run.

### 3. Run All Scenarios

Use the convenience script `run_scenario.py` from the project root. Run with 5 orders each for quick validation:

```bash
python run_scenario.py --scenario delayed-orders      --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario on-time-orders      --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario cancelled-orders     --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario duplicate-events     --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario eta-updated-orders   --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
python run_scenario.py --scenario mixed-orders         --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
```

Alternatively, from the simulator directory:

```bash
cd simulator
PYTHONPATH=src python -m order_simulator.main --scenario delayed-orders --orders-count 5 --kafka-bootstrap-servers localhost:9092 --orders-topic Orders
```

### 4. Verify Results

**Check SMS commands emitted:**

```bash
docker exec kafka bash -c "kafka-console-consumer --bootstrap-server kafka:29092 --topic sms-commands --from-beginning --max-messages 50 --timeout-ms 20000" 2>&1
```

**Check dead letter topic (should always be empty):**

```bash
docker exec kafka bash -c "kafka-console-consumer --bootstrap-server kafka:29092 --topic dead-letter-events --from-beginning --max-messages 10 --timeout-ms 5000" 2>&1
```

**Monitor the Flink UI:**
Open `http://localhost:8081` — check job status, checkpoints, and backpressure.

**Monitor Kafka topics via UI:**
Open `http://localhost:8080` — browse `Orders`, `sms-commands`, `dead-letter-events`.

---

## Expected Results Per Scenario

| Scenario | Orders | Expected SMS | Actual Behavior |
|----------|--------|-------------|----------------|
| **delayed-orders** | 5 | 5 SEND_DELAY_SMS | ✅ One per order, emitted after expectedDeliveryTime passes |
| **on-time-orders** | 5 | 0 | ✅ Order reaches DELIVERED before deadline, no SMS |
| **cancelled-orders** | 5 | 0 | ✅ Order reaches CANCELLED before deadline, no SMS |
| **duplicate-events** | 5 | 5 (not 10) | ✅ Duplicate events deduplicated, exactly one SMS per order |
| **eta-updated-orders** | 5 | 5 | ✅ SMS timed against the *updated* expectedDeliveryTime |
| **mixed-orders** | 5 | ~1-2 (varies) | ✅ Only delayed-type orders emit SMS |

---

## Common Issues and Solutions

### Checkpoint size not growing after scenario submission

This is normal — checkpoint size only grows from ~6855 bytes to ~8090 bytes when new events are processed. If checkpoints are still firing every 10s with steady size, the job is healthy but waiting for new data.

### "CoordinatorNotAvailableException" in TaskManager logs

Harmless transient error during Kafka startup. The consumer retries and connects once the coordinator is available. If you see `Discovered group coordinator` messages after it, recovery succeeded.

### kafka-init container exited with error

Expected if topics already exist from a previous run. The topics are still usable. Verify with:

```bash
docker exec kafka bash -c "kafka-topics --bootstrap-server kafka:29092 --list"
```

Should show: `Orders`, `sms-commands`, `dead-letter-events`

### SMS not appearing immediately

Timers are based on `expectedDeliveryTime` (processing time). For `delayed-orders`, the scenario sets `expectedDeliveryTime` to `now+2s`, so SMS should appear within 2-3 seconds. For longer delays, check the scenario's time expressions.

### "TimeoutException" from kafka-console-consumer

The consumer prints all available messages then times out — this is normal. The `Processed a total of N messages` line shows the actual count.

---

## Testing Idempotency and Deduplication

To verify idempotency, run the **duplicate-events** scenario and check that the number of SMS commands equals the number of unique order IDs (not the number of events sent). Each message on `sms-commands` has a `commandId` of the form `orderId:DELAY_SMS` — no `commandId` should appear more than once.

---

## Failure Recovery Test

For manual failure recovery testing, see `docs/runbooks/failure-test-runbook.md`.

Quick flow:
1. Submit job, run `failure-recovery` scenario
2. Cancel the Flink job via UI or CLI
3. Re-submit the job (it will restore from the latest checkpoint)
4. Verify exactly one SMS per delayed order — no duplicates after recovery

---

## Tear Down

```bash
docker compose down
```

To also remove all Kafka data and Flink checkpoints:

```bash
docker compose down -v