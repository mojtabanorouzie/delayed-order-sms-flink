# Scaling Runbook

Guidance for scaling individual jobs, tuning parallelism, and managing capacity for the five-job Flink setup.

---

## Parallelism basics

Each job is submitted with `--parallelism N`. This sets:
- The number of parallel subtasks for each operator in the job
- The number of Kafka source partitions consumed concurrently

**Rule:** `parallelism <= topic partition count`. Setting parallelism higher than the partition count wastes task slots — extra subtasks sit idle with no assigned partition.

| Job | Output topic partitions | Recommended max parallelism |
|---|---|---|
| SMS | `sms-commands` (3) — but source `Orders` has 3 | 3 |
| Refund | `refund-commands` (8) — source `Orders` has 3 | 3 |
| Courier | `courier-pause-commands` (8) — source `Orders` has 3 | 3 |
| Restaurant | `restaurant-alerts` (4) — source `Orders` has 3 | 3 |
| Surge | `surge-pricing-signals` (8) — source `Orders` has 3, `weather-data` has 4 | 3 |

To scale beyond these limits, increase the source topic partition count first, then the job parallelism.

---

## When to scale up

Scale a job when you observe:
- Kafka consumer group lag growing continuously (job can't keep up with input)
- High CPU utilization on the single task slot
- Back-pressure indicators in the Flink UI (red bars on operator I/O)

Check consumer group lag:

```bash
# Check lag for each job's consumer group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group delayed-order-sms-flink

docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group auto-refund-flink

docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group courier-overload-flink

docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group restaurant-bottleneck-flink

docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group surge-pricing-flink
```

A **LAG** value that is consistently growing (not just a transient spike) means the job cannot keep up and needs to be scaled.

---

## Scaling a job

### Step 1 — Increase topic partitions (if needed)

```bash
# Increase Orders topic to 6 partitions (to support parallelism 6)
docker exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --alter --topic Orders \
  --partitions 6
```

Partition count can only increase, never decrease. Kafka will rebalance partition assignments automatically.

### Step 2 — Take a savepoint

```bash
# List running jobs
docker exec flink-jobmanager flink list

# Save state before rescaling
docker exec flink-jobmanager flink savepoint <JOB_ID> \
  file:///opt/flink/checkpoints/savepoints
```

### Step 3 — Cancel the job

```bash
docker exec flink-jobmanager flink cancel <JOB_ID>
```

### Step 4 — Ensure the Flink cluster has enough task slots

Each subtask needs one task slot. The TaskManager must have `taskmanager.numberOfTaskSlots` ≥ (total parallelism across all jobs).

In `docker-compose.yml`:
```yaml
taskmanager:
  environment:
    - FLINK_PROPERTIES=taskmanager.numberOfTaskSlots: 10
```

### Step 5 — Resubmit with higher parallelism

```bash
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  /tmp/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --parallelism 3 \
  -s file:///opt/flink/checkpoints/savepoints/<SAVEPOINT_DIR>
```

Flink rescales keyed state automatically when restoring from a savepoint with a different parallelism. Key-group assignment is recalculated and state is redistributed across the new number of subtasks.

---

## State size per job

Monitor state size in the Flink UI under **Job → Overview → Subtasks → Bytes in RocksDB**.

Rough estimates per key:
- SMS / Refund: ~100 bytes per `orderId` (two small ValueStates)
- Courier: ~50 bytes per active order entry in `MapState`; variable per `courierId`
- Restaurant: ~50 bytes per accepted order entry in `MapState`; variable per `storeId`
- Surge: ~500 bytes per `zoneId` (WeatherData + SurgeWindowState + Double)

For 1 million in-flight orders with 7-day TTL, expect:
- SMS state: ~100 MB per task slot at parallelism 1
- Courier state depends on active orders per courier; with 10 active orders avg, ~500 bytes per courier

RocksDB manages this off-heap, so heap size (`jobmanager.memory.heap.size`) does not need to grow proportionally. Increase RocksDB block cache (`state.backend.rocksdb.memory.managed: true`) if read amplification is observed.

---

## Incremental checkpoint tuning

Checkpoint performance degrades if too many SST files accumulate. Tune RocksDB compaction if needed.

In `docker-compose.yml` environment for the TaskManager:

```yaml
- FLINK_PROPERTIES=state.backend.rocksdb.compaction.style: LEVEL
  state.backend.rocksdb.use-bloom-filter: true
  state.backend.rocksdb.memory.managed: true
```

`memory.managed: true` lets Flink manage the total RocksDB memory budget across all state backends on a TaskManager, preventing OOM from multiple jobs.

---

## Scaling the Flink cluster itself

To add more TaskManagers (horizontal scaling):

In Docker Compose:

```bash
docker compose up -d --scale taskmanager=3
```

Each TaskManager registers with the JobManager automatically. New slots are available for job submissions without restarting existing jobs.

For production Kubernetes deployments, scale the `TaskManager` `Deployment` replicas. Flink Native Kubernetes and the Flink Operator support reactive scaling via `flink.autoscaler`.

---

## Capacity planning summary

| Scenario | Recommended config |
|---|---|
| Development / demo | parallelism=1, 1 TaskManager with 4 slots |
| Single-region production (< 10k orders/min) | parallelism=2, 2 TaskManagers with 4 slots each |
| Multi-region production (> 100k orders/min) | parallelism=6+, ≥4 TaskManagers, Orders topic ≥ 6 partitions |
| Surge pricing under high weather variability | Separate deployment with dedicated TaskManager to isolate CoProcessFunction latency |

---

## Rollback procedure

If a scaled-up job shows unexpected behaviour:

```bash
# 1. Take a new savepoint at current (scaled-up) state
docker exec flink-jobmanager flink savepoint <JOB_ID> \
  file:///opt/flink/checkpoints/savepoints

# 2. Cancel
docker exec flink-jobmanager flink cancel <JOB_ID>

# 3. Resubmit at original parallelism, restoring from savepoint
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.<ClassName> \
  /tmp/delayed-order-sms-flink-job.jar \
  --parallelism 1 \
  -s file:///opt/flink/checkpoints/savepoints/<SAVEPOINT_DIR>
```

Flink handles downscaling from the savepoint the same as upscaling — key-group state is re-merged into the smaller number of subtasks.
