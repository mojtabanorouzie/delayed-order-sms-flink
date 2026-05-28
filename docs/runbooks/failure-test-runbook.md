# Failure Test Runbook

This runbook provides step-by-step procedures for testing the failure recovery behavior of the Delayed Order SMS Flink job.

---

## Objectives

Verify that the Flink job correctly:

1. **Recovers from checkpoint after a failure** — state restored, no data loss.
2. **Does not emit duplicate SMS** — idempotency key prevents duplicates across restarts.
3. **Handles Kafka broker outages** — job restarts and resumes from last checkpoint.
4. **Handles TaskManager failures** — Flink redistributes work.

---

## Prerequisites

- Docker environment running (see `local-runbook.md` for setup)
- Flink job submitted and running
- Kafka UI available at `http://localhost:8080`

---

## Test 1: Job Cancellation and Restart with Checkpoint Recovery

### Goal
Verify that after a job is cancelled and restarted from the last checkpoint, previously emitted SMS commands are not duplicated.

### Steps

```bash
# 1. Start infrastructure and submit job (if not already running)
docker compose up -d
# Wait for healthy services, then submit JAR via Flink UI

# 2. Run duplicate-events scenario to seed state
python run_scenario.py duplicate-events --orders-count 5

# 3. Wait for processing (10 seconds) and note SMS count in Kafka UI
# Record observed count: ___

# 4. Cancel the job from Flink UI
# Flink UI → Running Jobs → click job → "Cancel" button
# Or via CLI:
JOB_ID=$(curl -s http://localhost:8081/jobs | python -c "import sys,json; print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -X PATCH "http://localhost:8081/jobs/${JOB_ID}"

# 5. Verify job is cancelled
curl -s http://localhost:8081/jobs | python -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; print('Running jobs:', len(jobs))"
# Expected: 0 running jobs

# 6. Check that checkpoint files exist
ls -la flink-checkpoints/
# Should see checkpoint directories with metadata and state files

# 7. Restart the job from the last checkpoint
# Re-upload the JAR and submit, OR if using savepoint:
curl -X POST "http://localhost:8081/jars/${JAR_ID}/run" \
  -H "Content-Type: application/json" \
  -d '{"savepointPath": "file:///opt/flink/data/checkpoints"}'

# 8. Re-run duplicate-events scenario with same orders
python run_scenario.py duplicate-events --orders-count 5

# 9. Check SMS count in Kafka UI again
# EXPECTED: Total SMS count should be 5 (NOT 10)
# If count > 5, idempotency has failed
```

### Acceptance Criteria
- [ ] Job successfully restarts from checkpoint
- [ ] Total SMS count = expected (no duplicates)
- [ ] `delaySmsEmitted` state boolean prevents re-emission
- [ ] `delayed_orders_detected` counter increments only for orders without prior SMS

---

## Test 2: Kafka Broker Outage During Processing

### Goal
Verify that when Kafka becomes unavailable, the Flink job fails gracefully, restarts, and resumes from the last checkpoint.

### Steps

```bash
# 1. Ensure job is running with healthy checkpoints
# Flink UI → Running Job → Checkpoints → should show successful checkpoints

# 2. Run delayed-orders scenario to create pending orders
python run_scenario.py delayed-orders --orders-count 3

# 3. Stop Kafka container
docker compose stop kafka

# 4. Observe Flink job behavior
docker compose logs jobmanager --tail 20
docker compose logs taskmanager --tail 20
# Expected: Connection errors, job may fail or enter restart loop

# 5. Wait 60 seconds, then restart Kafka
docker compose start kafka

# 6. Wait for Kafka to be ready
until docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 5
done

# 7. Observe Flink job recovery
# Flink UI → Running Jobs → job should be RUNNING again
# Checkpoint should restore successfully

# 8. Run another scenario to verify processing
python run_scenario.py delayed-orders --orders-count 2

# 9. Verify new SMS commands are produced
# Kafka UI → sms-commands → should have 5 total (3 + 2)
```

### Acceptance Criteria
- [ ] Flink job detects Kafka outage and attempts restart
- [ ] After Kafka recovers, job restores from checkpoint
- [ ] Processing resumes without manual intervention
- [ ] No data loss (all orders processed)

---

## Test 3: TaskManager Failure

### Goal
Verify that when a TaskManager crashes, Flink redistributes work and recovers state.

### Steps

```bash
# 1. Ensure job is running with parallelism >= 2
# Submit with --parallelism 2

# 2. Scale TaskManagers to 2 replicas (if not already)
docker compose up -d --scale taskmanager=2

# 3. Run a scenario to create workload
python run_scenario.py delayed-orders --orders-count 10

# 4. Identify a TaskManager to kill
docker compose ps taskmanager
# Pick one container

# 5. Kill the TaskManager
docker compose stop taskmanager-1  # or appropriate container name

# 6. Observe Flink behavior
docker compose logs jobmanager --tail 30
# Expected: JobManager detects TaskManager loss, redistributes partitions

# 7. Check Flink UI
# TaskManagers tab → should show 1 remaining TM
# Job → should show RESTARTING or RUNNING

# 8. Wait 30 seconds for recovery

# 9. Verify job is RUNNING again
curl -s http://localhost:8081/jobs | python -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; print(jobs[0]['state'])"
# Expected: RUNNING

# 10. Run another scenario to confirm processing works
python run_scenario.py delayed-orders --orders-count 5

# 11. Check SMS count — should NOT have duplicates for orders processed before failure
```

### Acceptance Criteria
- [ ] JobManager detects TaskManager failure within 60 seconds
- [ ] State is restored from checkpoint (no loss)
- [ ] Processing continues on remaining TaskManager
- [ ] No duplicate SMS for orders processed before the failure

---

## Test 4: State Corruption Recovery

### Goal
Verify that if RocksDB state is corrupted, the job can recover from the last healthy checkpoint.

### Steps

```bash
# 1. Ensure checkpoints are being created
# Flink UI → Running Job → Checkpoints → note the latest checkpoint path

# 2. Run a scenario to populate state
python run_scenario.py delayed-orders --orders-count 5

# 3. Cancel the job
# Flink UI → Cancel

# 4. Corrupt the state (simulate)
CHECKPOINT_DIR="flink-checkpoints"
# Back up the original checkpoint first
cp -r "$CHECKPOINT_DIR" "${CHECKPOINT_DIR}_backup"

# Simulate corruption by deleting the latest checkpoint metadata
LATEST=$(ls -t "$CHECKPOINT_DIR" | head -1)
rm -rf "${CHECKPOINT_DIR:?}/${LATEST:?}"

# 5. Restart the job
# Submit JAR — Flink will use the next available checkpoint

# 6. Verify the job starts
# Flink UI → Running Jobs → should be RUNNING

# 7. Run a new scenario
python run_scenario.py delayed-orders --orders-count 3

# 8. Verify SMS produced for new orders only (not re-emitted for corrupted state orders)
```

### Acceptance Criteria
- [ ] Job starts even with corrupted latest checkpoint (falls back to previous)
- [ ] Processing works correctly
- [ ] Data in undamaged checkpoints is preserved

---

## Test 5: Full Clean Restart (No Checkpoint)

### Goal
Verify behavior when the job starts fresh with no prior state.

### Steps

```bash
# 1. Tear down everything
docker compose down -v

# 2. Recreate infrastructure
docker compose up -d

# 3. Submit the job fresh (no savepoint/checkpoint)
# Submit JAR via Flink UI without specifying a savepoint path

# 4. Run duplicate-events scenario
python run_scenario.py duplicate-events --orders-count 5

# 5. Check SMS count
# EXPECTED: 5 SMS commands (dedup within this run only)

# 6. Restart the scenario
python run_scenario.py duplicate-events --orders-count 5

# 7. Check SMS count again
# EXPECTED: 10 total (5 + 5) because previous `delaySmsEmitted` state was lost
# This is EXPECTED behavior — downstream deduplication by commandId prevents actual SMS
```

### Acceptance Criteria
- [ ] Fresh start works without errors
- [ ] `delaySmsEmitted` state is empty on fresh start
- [ ] Re-processed orders may produce duplicate commands (acceptable — downstream handles it)
- [ ] Job processes all events correctly

---

## Cleanup After Testing

```bash
# Stop all containers and remove volumes
docker compose down -v

# Clean up any backup files
rm -rf flink-checkpoints_backup

# Verify clean state
docker compose ps
# Should show no containers running
```

---

## Test Report Template

| Test Case | Date | Tester | Pass/Fail | SMS Count | Notes |
|-----------|------|--------|-----------|-----------|-------|
| Test 1: Checkpoint Recovery | | | | | |
| Test 2: Kafka Outage | | | | | |
| Test 3: TaskManager Failure | | | | | |
| Test 4: State Corruption | | | | | |
| Test 5: Fresh Start | | | | | |