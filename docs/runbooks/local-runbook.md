# Local Development Runbook

This runbook describes how to set up, run, and troubleshoot the Delayed Order SMS Flink job in a local Docker environment.

---

## Prerequisites

| Tool | Minimum Version | Check Command |
|------|----------------|---------------|
| Docker | 24.0+ | `docker --version` |
| Docker Compose | v2 (plugin) | `docker compose version` |
| Java JDK | 17 | `javac --version` |
| Maven | 3.9+ | `mvn --version` |
| Python | 3.10+ | `python --version` |
| Git | Any | `git --version` |

### Resource Requirements

- **RAM:** At least 8 GB free (Kafka + Flink JobManager + Flink TaskManager)
- **Disk:** At least 5 GB free (Docker images + Kafka data + Flink checkpoints)

---

## Quick Start

### 1. Clone and Navigate

```bash
cd delayed-order-sms-flink
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- **Kafka** (with KRaft mode, no ZooKeeper) on `localhost:9092`
- **Kafka UI** on `http://localhost:8080`
- **Flink JobManager** on `http://localhost:8081`
- **Flink TaskManager** (1 replica, connects to JobManager)

### 3. Wait for Healthy Services

```bash
# Check all services are running and healthy
docker compose ps
```

Expected output: All services show `running` status. Kafka and Flink may take 30-60 seconds to become fully operational.

You can also verify via:
- **Flink UI:** Open `http://localhost:8081` — should show Flink Dashboard with 1 TaskManager registered.
- **Kafka UI:** Open `http://localhost:8080` — should show cluster connected.

### 4. Build the Flink Job JAR

```bash
mvn clean package -f flink-job/pom.xml -DskipTests
```

The output JAR is at: `flink-job/target/delayed-order-sms-flink-job.jar`

### 5. Submit Job to Flink

#### Option A: Via Flink UI (Recommended for Development)

1. Open `http://localhost:8081`
2. Click **"Submit New Job"** → **"Add New"**
3. Upload `flink-job/target/delayed-order-sms-flink-job.jar`
4. Click the uploaded JAR
5. Set parallelism: `2`
6. Click **"Submit"**

#### Option B: Via REST API

```bash
# Upload JAR
JAR_ID=$(curl -s -X POST -H "Expect:" \
  -F "jarfile=@flink-job/target/delayed-order-sms-flink-job.jar" \
  http://localhost:8081/jars/upload | python -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")

echo "Uploaded JAR ID: $JAR_ID"

# Submit job
curl -X POST "http://localhost:8081/jars/${JAR_ID}/run"
```

### 6. Verify Job is Running

Open `http://localhost:8081/#/job/running` — the job "Delayed Order SMS Detection Job" should appear with status `RUNNING`.

If you see `FAILED` or a restart loop, check the Flink logs:

```bash
docker compose logs jobmanager
docker compose logs taskmanager
```

### 7. Install Simulator Dependencies

```bash
pip install -r simulator/requirements.txt
```

### 8. Run a Test Scenario

```bash
python run_scenario.py delayed-orders --orders-count 5
```

This sends 5 delayed order events to Kafka. The Flink job should detect them and emit SMS commands.

### 9. Verify Results in Kafka UI

1. Open `http://localhost:8080`
2. Go to **Topics** → select `sms-commands`
3. Click **"Messages"** and start consuming
4. You should see JSON messages with `commandType: "SEND_DELAY_SMS"`

### 10. Check Flink Metrics

1. Open `http://localhost:8081`
2. Click the running job → **"Metrics"** tab
3. Look for custom counters: `delayed_orders_detected`, `sms_commands_emitted`, etc.

### 11. Tear Down

```bash
docker compose down -v
```

This stops all containers and removes volumes (Kafka data, Flink checkpoints).

---

## Available Scenarios

| Scenario | Command | Description |
|----------|---------|-------------|
| `delayed-orders` | `python run_scenario.py delayed-orders --orders-count 5` | Orders with ETA in the past — should trigger SMS |
| `on-time-orders` | `python run_scenario.py on-time-orders --orders-count 5` | Orders with ETA far in the future — no SMS |
| `cancelled-orders` | `python run_scenario.py cancelled-orders --orders-count 5` | Orders that are cancelled — no SMS |
| `duplicate-events` | `python run_scenario.py duplicate-events --orders-count 5` | Duplicate events — only one SMS per order |
| `eta-updated-orders` | `python run_scenario.py eta-updated-orders --orders-count 5` | ETA changed — SMS for final ETA only |
| `mixed-orders` | `python run_scenario.py mixed-orders --orders-count 5` | Mix of all types |

---

## Troubleshooting

### Port Conflicts

**Symptom:** `docker compose up` fails with "port is already allocated"

```bash
# Check what's using the ports
netstat -ano | findstr "8080 8081 9092"

# Change ports in docker-compose.yml or stop conflicting services
```

### Build Failures

**Symptom:** `mvn clean package` fails with "No compiler is provided in this environment"

**Cause:** JAVA_HOME is not set, or a JRE is being used instead of a JDK.

```bash
# On Windows (PowerShell)
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
# On Linux/macOS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
```

**Symptom:** `mvn clean package` fails with missing dependencies

```bash
# Force re-download dependencies
mvn clean package -f flink-job/pom.xml -U
```

### Kafka Not Ready

**Symptom:** Simulator fails with "No broker available" or "Timeout connecting to Kafka"

```bash
# Wait for Kafka to be fully ready
docker compose logs kafka | grep "Transition from STARTING to STARTED"

# Or poll until ready
until docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 5
done
echo "Kafka is ready!"
```

### Flink Job Fails to Start

**Symptom:** Job status shows `FAILED` in Flink UI

```bash
# Check Flink logs
docker compose logs jobmanager --tail 100
docker compose logs taskmanager --tail 100

# Common issues:
# 1. Kafka not reachable → ensure Kafka is healthy
# 2. Checkpoint directory permissions → check docker-compose volumes
# 3. JAR compatibility → ensure Flink version matches (1.19.1)
```

### Topics Not Auto-Created

**Symptom:** Simulator fails with "Topic 'Orders' not found"

The `Orders` topic should be auto-created. If not:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic Orders \
  --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact
```

### State/Checkpoint Issues

**Symptom:** "Checkpoint expired" or job can't restore

```bash
# Clear all state and start fresh
docker compose down -v
docker compose up -d
```

---

## Environment Variables

The Flink job accepts these CLI arguments (configurable in the Flink UI submit form):

```
--kafka-bootstrap-servers kafka:9092
--orders-topic Orders
--sms-commands-topic sms-commands
--consumer-group-id delayed-order-sms-job
--parallelism 2
--checkpoint-interval-ms 30000
--checkpoint-storage-path file:///opt/flink/data/checkpoints
--restart-attempts 10
--restart-delay-ms 10000
--state-ttl-days 7