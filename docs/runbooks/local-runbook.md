# Local Development Runbook

Setup, development workflow, and troubleshooting for all five Flink jobs in the local Docker environment.

---

## Prerequisites

| Tool | Version | Check |
|---|---|---|
| Docker + Compose | 24.0+ / v2 plugin | `docker compose version` |
| Java JDK | 17 | `javac --version` |
| Maven | 3.9+ | `mvn --version` |
| Python | 3.9+ | `python --version` |

**Resources:** 8 GB RAM free, 5 GB disk free.

---

## Quick Start — Automated

The fastest path is the e2e runner. It handles everything: infrastructure, JAR build, job submission, scenario execution, and teardown.

```bash
python e2e-tests/run_e2e.py
# Expected runtime: ~6 minutes
# Expected: all scenarios pass, 5 jobs RUNNING
```

Leave infrastructure up after the run:

```bash
python e2e-tests/run_e2e.py --no-cleanup
```

---

## Manual Setup — Step by Step

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps   # wait until all services show healthy
```

Services started:
- **Kafka** (KRaft, no ZooKeeper) on `localhost:9092`
- **Kafka UI** on `http://localhost:8080`
- **Flink JobManager** on `http://localhost:8081`
- **Flink TaskManager** (connected to JobManager)
- **kafka-init** (one-shot: creates all 8 topics, then exits)

Allow 30–60 seconds for Kafka and Flink to become healthy before proceeding.

### 2. Build the fat JAR

```bash
mvn clean package -f flink-job/pom.xml -DskipTests
# Output: flink-job/target/delayed-order-sms-flink-job.jar
```

### 3. Copy JAR into jobmanager container

```bash
docker cp flink-job/target/delayed-order-sms-flink-job.jar \
  flink-jobmanager:/tmp/delayed-order-sms-flink-job.jar
```

### 4. Submit all five jobs

```bash
JAR=/tmp/delayed-order-sms-flink-job.jar
BASE="--kafka.bootstrap.servers kafka:29092 --orders.topic Orders \
      --checkpoint.storage.path file:///opt/flink/checkpoints --parallelism 1"

# Job 1 — Delayed Order SMS
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.DelayedOrderSmsJob $JAR $BASE \
  --sms.commands.topic sms-commands --consumer.group.id delayed-order-sms-flink

# Job 2 — Auto-Refund
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.AutoRefundJob $JAR $BASE \
  --refund.commands.topic refund-commands --consumer.group.id auto-refund-flink \
  --refund.delay.minutes 30

# Job 3 — Courier Overload
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.CourierOverloadJob $JAR $BASE \
  --courier.commands.topic courier-pause-commands \
  --consumer.group.id courier-overload-flink \
  --overload.threshold 8 --resume.threshold 5

# Job 4 — Restaurant Bottleneck
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.RestaurantBottleneckJob $JAR $BASE \
  --restaurant.alerts.topic restaurant-alerts \
  --consumer.group.id restaurant-bottleneck-flink \
  --window.size.seconds 300

# Job 5 — Dynamic Surge Pricing
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.SurgePricingJob $JAR $BASE \
  --surge.signals.topic surge-pricing-signals \
  --consumer.group.id surge-pricing-flink \
  --window.size.seconds 60

# Verify: expect 5 RUNNING
docker exec flink-jobmanager flink list
```

### 5. Install simulator dependencies

```bash
pip install -r simulator/requirements.txt
```

### 6. Run scenarios

```bash
# SMS scenarios
docker compose --profile e2e run --rm simulator \
  --scenario delayed-orders --orders-count 5 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders

# Courier overload
docker compose --profile e2e run --rm simulator \
  --scenario courier-overload --orders-count 10 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders

# Restaurant bottleneck
docker compose --profile e2e run --rm simulator \
  --scenario restaurant-bottleneck --orders-count 5 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders

# Surge pricing — inject weather first
docker exec kafka bash -c \
  "echo '{\"region\":\"zone-surge-e2e\",\"condition\":\"RAIN\",\"temperature\":15.0,\"windSpeed\":10.0,\"schemaVersion\":1}' \
   | kafka-console-producer --bootstrap-server kafka:29092 --topic weather-data"
docker compose --profile e2e run --rm simulator \
  --scenario surge-pricing --orders-count 5 \
  --kafka-bootstrap-servers kafka:29092 --orders-topic Orders
```

### 7. Consume output topics

```bash
# SMS commands
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic sms-commands \
  --from-beginning --timeout-ms 10000

# Refund commands
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic refund-commands \
  --from-beginning --timeout-ms 10000

# Courier pause/resume commands
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic courier-pause-commands \
  --from-beginning --timeout-ms 10000

# Restaurant alerts
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic restaurant-alerts \
  --from-beginning --timeout-ms 10000

# Surge pricing signals
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic surge-pricing-signals \
  --from-beginning --timeout-ms 15000

# Dead-letter (should be empty in healthy runs)
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic dead-letter-events \
  --from-beginning --timeout-ms 5000
```

### 8. Tear down

```bash
docker compose down -v
# -v removes volumes (Kafka data, Flink checkpoints)
```

---

## Development Iteration Loop

When changing job code:

```bash
# 1. Build (skip tests for speed)
mvn clean package -f flink-job/pom.xml -DskipTests

# 2. Copy new JAR
docker cp flink-job/target/delayed-order-sms-flink-job.jar \
  flink-jobmanager:/tmp/delayed-order-sms-flink-job.jar

# 3. Cancel old job (get job ID from Flink UI or flink list)
docker exec flink-jobmanager flink cancel <JOB_ID>

# 4. Re-submit the specific job you changed
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.<ClassName> \
  /tmp/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 ...
```

To run unit tests only (no Docker required):

```bash
mvn test -f flink-job/pom.xml
# Tests run: 100, Failures: 0
```

---

## Troubleshooting

### Port conflicts

```bash
# Windows
netstat -ano | findstr "8080 8081 9092"

# Linux/macOS
lsof -i :8080 -i :8081 -i :9092
```

Change ports in `docker-compose.yml` if needed.

### Job shows FAILED or RESTARTING

```bash
# Check logs
docker compose logs jobmanager --tail 50
docker compose logs taskmanager --tail 50
```

Common causes:
- Kafka not yet healthy — wait 30 seconds and retry
- Checkpoint directory permission denied — `docker compose down -v && docker compose up -d` to recreate volumes
- JAR not found at the path passed to `flink run` — re-copy the JAR

### `kafka-init` exited with error in `docker compose ps`

Expected if topics already exist. Verify all topics are present:

```bash
docker exec kafka kafka-topics \
  --bootstrap-server kafka:29092 --list
```

Expected 8 topics: `Orders`, `weather-data`, `sms-commands`, `refund-commands`, `courier-pause-commands`, `restaurant-alerts`, `surge-pricing-signals`, `dead-letter-events`.

### Surge signals not appearing

The SurgePricingJob uses `OffsetsInitializer.latest()` for the `weather-data` topic. Weather events injected before the job started are not consumed. Always inject weather after the job is running:

```bash
# Confirm surge job is RUNNING
docker exec flink-jobmanager flink list | grep Surge

# Then inject weather
docker exec kafka bash -c \
  "echo '{\"region\":\"zone-surge-e2e\",\"condition\":\"RAIN\",\"temperature\":15.0,\"windSpeed\":10.0,\"schemaVersion\":1}' \
   | kafka-console-producer --bootstrap-server kafka:29092 --topic weather-data"
```

### Restaurant alerts not appearing

The `--window.size.seconds` must be set to a value that fires within your test window. The default (300 seconds) is too long for manual testing. Use `--window.size.seconds 10` for local development.

### Build fails — No compiler / JDK not found

```powershell
# Windows PowerShell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

```bash
# Linux/macOS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
```

### Dependency download fails

```bash
mvn clean package -f flink-job/pom.xml -U -DskipTests
# -U forces dependency re-download
```

---

## Local Services

| Service | URL | What to check |
|---|---|---|
| Flink UI | http://localhost:8081 | 5 jobs RUNNING, no exceptions in job exceptions tab |
| Kafka UI | http://localhost:8080 | Topic browser, message inspector, consumer group lag |
| Kafka | localhost:9092 | Direct producer/consumer access |

---

## Available Scenarios

| Scenario file | Expected output |
|---|---|
| `delayed-orders` | 5 SMS commands on `sms-commands` |
| `on-time-orders` | 0 SMS commands |
| `cancelled-orders` | 0 SMS commands |
| `duplicate-events` | 5 SMS commands (no duplicates) |
| `eta-updated-orders` | 5 SMS commands |
| `mixed-orders` | 0–2 SMS commands |
| `severely-delayed-refund` | 5 refund commands on `refund-commands` |
| `courier-overload` | 1 PAUSE command on `courier-pause-commands` |
| `restaurant-bottleneck` | 1–5 CRITICAL alerts on `restaurant-alerts` |
| `surge-pricing` | 1–3 signals on `surge-pricing-signals` (after RAIN weather injected) |
