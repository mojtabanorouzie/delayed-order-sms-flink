# Production Runbook

This runbook covers the operation of the Delayed Order SMS Flink job in a production Kubernetes environment.

---

## 1. Architecture Overview

### Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka 3.6+ | Decouple order service from Flink; compacted `Orders` topic |
| **Stream Processor** | Apache Flink 1.19+ | Detect delayed orders, emit SMS commands |
| **State Backend** | RocksDB (EmbeddedRocksDBStateBackend) | Persistent keyed state with incremental checkpoints |
| **Orchestration** | Kubernetes + Flink Operator | Deploy, scale, and manage Flink clusters |
| **SMS Service** | External HTTP/gRPC service | Sends actual SMS; deduplicates by `commandId` |
| **Monitoring** | Prometheus + Grafana | Metrics collection and alerting |
| **Logging** | ELK Stack / Loki | Centralized log aggregation |

### Deployment Topology

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                          │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │ Flink    │  │ Flink    │  │ Flink        │  │ Kafka         │ │
│  │ Operator │  │ JobMgr   │  │ TaskMgr x N  │  │ (external or  │ │
│  │          │  │ (HA)     │  │ (autoscale)  │  │  Strimzi)     │ │
│  └──────────┘  └──────────┘  └──────────────┘  └────────────────┘ │
│                     │                 │                │            │
│                     └─────────┬───────┘                │            │
│                               │                        │            │
│                     ┌─────────▼─────────┐              │            │
│                     │ Checkpoint Store  │              │            │
│                     │ (S3 / HDFS / NFS) │              │            │
│                     └───────────────────┘              │            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Deployment

### 2.1 Prerequisites

- Kubernetes 1.27+
- `kubectl` configured with cluster access
- Helm 3.12+
- Flink Kubernetes Operator 1.7+
- Cert-Manager (for Flink Operator webhooks)

### 2.2 Flink Operator Installation

```bash
# Add Flink Operator Helm repo
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.7.0/
helm repo update

# Install operator
helm install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  --namespace flink-operator \
  --create-namespace \
  --set webhook.create=true
```

### 2.3 Flink Application Deployment

#### FlinkDeployment CR

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: delayed-order-sms
  namespace: flink-jobs
spec:
  image: registry.company.com/delayed-order-sms-flink:0.1.0
  flinkVersion: v1_19
  flinkConfiguration:
    state.backend.type: rocksdb
    state.backend.incremental: "true"
    state.checkpoints.dir: s3://flink-checkpoints/delayed-order-sms
    execution.checkpointing.interval: "30s"
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.min-pause: "5s"
    execution.checkpointing.timeout: "60s"
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: "10"
    restart-strategy.fixed-delay.delay: "10s"
    taskmanager.numberOfTaskSlots: "4"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
    replicas: 1  # Set to 2+ for HA
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 2
  job:
    jarURI: local:///opt/flink/usrlib/delayed-order-sms-flink-job.jar
    args:
      - --kafka-bootstrap-servers
      - kafka-broker-0.kafka:9092,kafka-broker-1.kafka:9092
      - --orders-topic
      - Orders
      - --sms-commands-topic
      - sms-commands
      - --parallelism
      - "4"
      - --state-ttl-days
      - "7"
    parallelism: 4
    upgradeMode: savepoint
```

### 2.4 Build and Push Docker Image

```bash
# Build the JAR
mvn clean package -f flink-job/pom.xml

# Build Docker image
docker build -t registry.company.com/delayed-order-sms-flink:0.1.0 \
  -f docker/Dockerfile .

# Push to registry
docker push registry.company.com/delayed-order-sms-flink:0.1.0
```

### 2.5 Submit Deployment

```bash
kubectl apply -f k8s/flink-deployment.yaml

# Watch deployment status
kubectl get flinkdeployment -n flink-jobs delayed-order-sms -w
```

---

## 3. Configuration

### 3.1 Job Arguments

| Parameter | Production Default | Description |
|-----------|-------------------|-------------|
| `--kafka-bootstrap-servers` | `kafka-broker-0.kafka:9092,...` | Kafka bootstrap servers |
| `--orders-topic` | `Orders` | Source topic (compacted) |
| `--sms-commands-topic` | `sms-commands` | SMS command sink topic |
| `--consumer-group-id` | `delayed-order-sms-prod` | Consumer group ID |
| `--parallelism` | `4` | Job parallelism (scale with load) |
| `--state-ttl-days` | `7` | State TTL for completed orders |

### 3.2 Flink Configuration

| Key | Value | Rationale |
|-----|-------|-----------|
| `taskmanager.numberOfTaskSlots` | `4` | 4 slots = 4 parallel subtasks per TM |
| `taskmanager.memory.process.size` | `4096m` | 4GB per TM; RocksDB needs ~1GB overhead |
| `jobmanager.memory.process.size` | `2048m` | 2GB for JM with HA metadata |
| `state.backend.rocksdb.memory.managed` | `true` | Let Flink manage RocksDB memory |
| `state.backend.rocksdb.localdir` | `/tmp/flink-rocksdb` | Local SSD for RocksDB data |
| `execution.checkpointing.interval` | `30s` | 30s = trade-off between overhead and recovery time |
| `execution.checkpointing.unaligned` | `false` | Aligned checkpoints for deterministic state |

### 3.3 Kafka Configuration

| Topic | Partitions | Replication Factor | Retention | Cleanup Policy |
|-------|-----------|-------------------|-----------|---------------|
| `Orders` | 12 | 3 | N/A | `compact` |
| `sms-commands` | 12 | 3 | 7 days | `delete` |
| `dead-letter-events` | 6 | 3 | 30 days | `delete` |

---

## 4. Monitoring

### 4.1 Metrics Pipeline

```
Flink Job → Prometheus PushGateway → Prometheus → Grafana Dashboards → Alerts → PagerDuty/Slack
```

### 4.2 Key Metrics Dashboard

Create a Grafana dashboard with the following panels:

#### Throughput Panel

```
PromQL: rate(flink_taskmanager_job_task_numRecordsInPerSecond{job_name="Delayed Order SMS Detection Job"}[5m])
```

#### Custom Counters Panel

```
PromQL:
delayed_orders_detected_total
sms_commands_emitted_total
stale_updates_ignored_total
invalid_messages_total
parse_errors_total
```

#### Checkpoint Health Panel

```
PromQL:
flink_jobmanager_job_lastCheckpointDuration
flink_jobmanager_job_lastCheckpointSize
flink_jobmanager_job_numberOfFailedCheckpoints
```

#### State Size Panel

```
PromQL: flink_taskmanager_job_task_managedMemoryUsed
```

### 4.3 Alerting Rules

| Alert | Condition | Severity | Channel |
|-------|-----------|----------|---------|
| `FlinkJobNotRunning` | `flink_jobmanager_job_uptime < 60` for 2m | **Critical** | PagerDuty |
| `NoDelayedOrdersDetected` | `rate(delayed_orders_detected_total[10m]) == 0` AND `rate(numRecordsInPerSecond[10m]) > 0` | **Warning** | Slack |
| `HighParseErrorRate` | `rate(parse_errors_total[5m]) > 0.1` | **Critical** | Slack |
| `CheckpointFailing` | `rate(flink_jobmanager_job_numberOfFailedCheckpoints[10m]) > 0` | **Warning** | Slack |
| `HighStaleUpdateRate` | `rate(stale_updates_ignored_total[5m]) > 10` | **Warning** | Slack |
| `StateSizeGrowing` | `flink_taskmanager_job_task_managedMemoryUsed` increase > 2x in 1h | **Warning** | Slack |
| `ConsumerLagHigh` | `kafka_consumer_lag{topic="Orders"} > 10000` | **Warning** | Slack |

---

## 5. Scaling

### 5.1 Horizontal Scaling

Increase `taskManager.replicas` in the FlinkDeployment CR:

```yaml
taskManager:
  replicas: 4  # Scale from 2 → 4
```

Apply and trigger savepoint-based upgrade:

```bash
kubectl apply -f k8s/flink-deployment.yaml
# Flink Operator handles savepoint → stop → restore → start with new parallelism
```

### 5.2 Vertical Scaling

Increase `taskManager.resource.memory` and `cpu`:

```yaml
taskManager:
  resource:
    memory: "8192m"
    cpu: 4
```

### 5.3 When to Scale

| Metric | Threshold | Action |
|--------|-----------|--------|
| CPU usage > 80% sustained | 5 min | Scale up (horizontal or vertical) |
| Records Out P99 latency > 500ms | 5 min | Investigate bottleneck; scale if needed |
| Consumer lag > 10k | 10 min | Scale up |
| State size > 10GB per TM | N/A | Scale up vertically |

---

## 6. Backup & Restore

### 6.1 Checkpoint Storage

Checkpoints are stored in S3 (or HDFS/NFS):

```
s3://flink-checkpoints/delayed-order-sms/<job-id>/chk-<checkpoint-id>/
```

### 6.2 Manual Savepoint

```bash
# Trigger savepoint
JOB_ID=$(kubectl exec -n flink-jobs deploy/flink-jobmanager -- \
  curl -s http://localhost:8081/jobs | \
  python -c "import sys,json; print(json.load(sys.stdin)['jobs'][0]['id'])")

kubectl exec -n flink-jobs deploy/flink-jobmanager -- \
  curl -X POST "http://localhost:8081/jobs/${JOB_ID}/savepoints" \
  -d '{"target-directory": "s3://flink-savepoints/delayed-order-sms", "cancel-job": false}'
```

### 6.3 Restore from Savepoint

```yaml
# In FlinkDeployment CR
job:
  initialSavepointPath: s3://flink-savepoints/delayed-order-sms/savepoint-xxxxx
  upgradeMode: savepoint
```

### 6.4 S3 Lifecycle Policy

Configure S3 lifecycle rules to manage checkpoint storage costs:

```json
{
  "Rules": [
    {
      "Id": "ExpireOldCheckpoints",
      "Status": "Enabled",
      "Filter": { "Prefix": "delayed-order-sms/" },
      "Expiration": { "Days": 30 }
    }
  ]
}
```

---

## 7. Incident Response

### 7.1 Incident: Job Not Processing Events

**Symptoms:** `numRecordsInPerSecond` = 0, consumer lag climbing.

**Investigation:**

```bash
# 1. Check job status
kubectl get flinkdeployment -n flink-jobs delayed-order-sms

# 2. Check Flink logs
kubectl logs -n flink-jobs deploy/flink-jobmanager --tail 100
kubectl logs -n flink-jobs deploy/flink-taskmanager --tail 100

# 3. Check Kafka connectivity
kubectl exec -n flink-jobs deploy/flink-jobmanager -- \
  nc -zv kafka-broker-0.kafka 9092

# 4. Check for exceptions in logs
kubectl logs -n flink-jobs deploy/flink-taskmanager --tail 200 | grep -i "error\|exception"
```

**Remediation:**

| Cause | Action |
|-------|--------|
| Kafka broker down | Contact Kafka team; Flink will auto-restart when Kafka recovers |
| State backend error | Check S3 connectivity and permissions |
| Out of memory | Scale up TaskManagers vertically |
| Configuration error | Fix FlinkDeployment CR, apply savepoint-based upgrade |

### 7.2 Incident: Duplicate SMS Detected

**Symptoms:** Customer reports receiving >1 SMS for same order delay.

**Investigation:**

```bash
# 1. Check `sms_commands_emitted` metric for unexpected spikes

# 2. Check `delaySmsEmitted` state (requires state debugging)
# Enable state access via Flink UI or Flink State Processor API

# 3. Check `stale_updates_ignored` for drops (indicating stale detection failing)

# 4. Review Kafka sms-commands topic for duplicate commandIds
kubectl exec -n kafka deploy/kafka-client -- \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sms-commands --from-beginning \
  --property print.key=true | grep "ORDER_ID" | sort | uniq -c
```

**Remediation:**

- Primary defense: Downstream SMS service **must** deduplicate by `commandId`. This is the ultimate safeguard.
- Check if `delaySmsEmitted` boolean is being set correctly in state.
- Verify no bugs in the `emitDelaySms()` method bypassing the boolean check.

### 7.3 Incident: High DLQ Volume

**Symptoms:** `parse_errors` or `invalid_messages` counters spiking.

**Investigation:**

```bash
# Consume dead-letter-events topic
kubectl exec -n kafka deploy/kafka-client -- \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic dead-letter-events --from-beginning --max-messages 10

# Check patterns in invalid events
```

**Remediation:**

| Error Type | Cause | Action |
|-----------|-------|--------|
| Parse errors | Upstream producing malformed JSON | Contact Order Service team; fix producer |
| Invalid events (null orderId) | Data quality issue | Fix upstream; add validation at producer |
| Stale updates | Duplicate/old events being sent | Check upstream event sourcing logic |

---

## 8. Rollback

### 8.1 Rollback to Previous Version

```yaml
# In FlinkDeployment CR, revert image tag
spec:
  image: registry.company.com/delayed-order-sms-flink:0.0.9  # Previous stable version
```

Apply with savepoint-based upgrade:

```bash
kubectl apply -f k8s/flink-deployment.yaml
# Flink Operator: savepoint → stop → deploy old version → restore from savepoint
```

### 8.2 Emergency Stop

```bash
# Suspend deployment (creates savepoint automatically)
kubectl patch flinkdeployment -n flink-jobs delayed-order-sms \
  --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
```

### 8.3 Resume After Emergency Stop

```bash
kubectl patch flinkdeployment -n flink-jobs delayed-order-sms \
  --type merge -p '{"spec":{"job":{"state":"running"}}}'
```

---

## 9. Runbook Quick Reference

| Task | Command |
|------|---------|
| View job status | `kubectl get flinkdeployment -n flink-jobs delayed-order-sms` |
| View Flink UI (port-forward) | `kubectl port-forward -n flink-jobs svc/flink-jobmanager-rest 8081:8081` |
| View JM logs | `kubectl logs -n flink-jobs deploy/flink-jobmanager --tail 100 -f` |
| View TM logs | `kubectl logs -n flink-jobs deploy/flink-taskmanager --tail 100 -f` |
| Trigger savepoint | See Section 6.2 |
| Suspend job | `kubectl patch flinkdeployment ... {"spec":{"job":{"state":"suspended"}}}` |
| Resume job | `kubectl patch flinkdeployment ... {"spec":{"job":{"state":"running"}}}` |
| Scale TMs | Edit `taskManager.replicas` in CR and apply |
| Check Kafka lag | Grafana dashboard or `kafka-consumer-groups --describe` |
| View DLQ messages | Consume from `dead-letter-events` topic |