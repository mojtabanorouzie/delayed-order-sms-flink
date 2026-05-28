# Agent Prompt: Fix Delayed Order SMS Flink Project

> **Instructions:** Copy everything below this line as a single prompt to an AI coding agent (Cline, Copilot, etc.) in ACT mode. The agent should execute subtasks sequentially, committing after each one.

---

## PROJECT CONTEXT

You are working in `g:\Projects\PersonalWebsite\delayed-order-sms-flink`. This is an Apache Flink streaming job that:

1. **Reads** order state events from Kafka topic `Orders` (compacted, keyed by `orderId`)
2. **Processes** them with a `KeyedProcessFunction` using processing-time timers
3. **Emits** SMS delay commands to Kafka topic `sms-commands` when `expectedDeliveryTime` passes
4. **Uses** a Python simulator to generate test order events
5. **Infrastructure** is defined in `docker-compose.yml` (Kafka + Flink JobManager/TaskManager)

---

## SUBTASKS — EXECUTE IN ORDER

### TASK 1 OF 12: Fix State Backend Mismatch (CRITICAL)

**File:** `flink-job/src/main/java/com/company/delayedordersms/DelayedOrderSmsJob.java`

**What to do:**
- Remove: `import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;`
- Replace: `env.setStateBackend(new HashMapStateBackend());`
- With: `env.setStateBackend(new EmbeddedRocksDBStateBackend(true));`
- Add: `import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;`
- Add dependency in `flink-job/pom.xml` if not present:
  ```xml
  <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-statebackend-rocksdb</artifactId>
      <version>${flink.version}</version>
  </dependency>
  ```
- Verify build: `mvn clean package -f flink-job/pom.xml`

**AC:** `EmbeddedRocksDBStateBackend` used, matches docker-compose RocksDB config, `mvn clean package` succeeds.

---

### TASK 2 OF 12: Write ALL Empty Documentation Files (CRITICAL)

**Files to create/write (all currently 1-byte stubs):**

1. `docs/adr/0001-processing-time-vs-event-time.md`
2. `docs/adr/0002-sms-idempotency-strategy.md`
3. `docs/rfc/delayed-order-detection-rfc.md`
4. `docs/runbooks/local-runbook.md`
5. `docs/runbooks/failure-test-runbook.md`
6. `docs/runbooks/production-runbook.md`
7. `proposal/production-proposal.md`

**Content specs:**

**ADR-0001** — Standard ADR format: Title, Status (Accepted), Context (why processing-time over event-time), Decision (use processing-time timers), Consequences (simpler code, no watermark/late-data complexity, non-deterministic replay, ok for POC). Mention that `WatermarkStrategy.noWatermarks()` is used in the source.

**ADR-0002** — Standard ADR format: Title, Status (Accepted), Context (SMS must not duplicate), Decision (`commandId = orderId + ":DELAY_SMS"`, `delaySmsEmitted` boolean in state), Consequences (idempotent by key, downstream must deduplicate, no at-least-once issue).

**RFC** — Full architecture document with: Overview, Architecture Diagram (Mermaid or ASCII), Data Flow, Input/Output Topic Contracts, State Model, Timer Semantics, Scalability, Failure Modes, Monitoring Requirements.

**Local Runbook** — Prerequisites (Docker, Java 17, Maven 3.9+, Python 3.10+), Step-by-step: `docker compose up -d`, wait healthy, `mvn clean package -f flink-job/pom.xml`, submit JAR to Flink UI, run simulator: `python run_scenario.py delayed-orders --orders-count 5`, check Kafka UI at `http://localhost:8080`, tear down: `docker compose down -v`. Include troubleshooting for common issues (port conflicts, build failures, Kafka not ready).

**Failure Test Runbook** — Steps: Start job, run scenario, cancel job from Flink UI, restart job from last checkpoint, verify recovery, verify no duplicate SMS. Include exact CLI commands.

**Production Runbook** — Sections: Architecture Overview, Deployment (K8s + Flink Operator), Configuration, Monitoring (metrics + alerts), Scaling, Backup/Restore, Incident Response, Rollback.

**Production Proposal** — Business case, Architecture, Infrastructure Requirements, Cost Estimate, Migration Plan, Risk Assessment.

---

### TASK 3 OF 12: Add Dead Letter Queue for Invalid Events (HIGH)

**Files to modify/create:**

1. **NEW:** `flink-job/src/main/java/com/company/delayedordersms/model/DeadLetterEvent.java`
   - Fields: `String originalPayload`, `String errorReason`, `String sourceTopic`, `Instant occurredAt`
   - Standard getters/setters, `Serializable`

2. **MODIFY:** `flink-job/src/main/java/com/company/delayedordersms/serde/OrderStateDeserializationFunction.java`
   - Add a side output: `public static final OutputTag<DeadLetterEvent> DEAD_LETTER_TAG = new OutputTag<>("dead-letter") {};`
   - On parse failure: emit `DeadLetterEvent` to side output instead of silently skipping
   - Keep the main output (valid `OrderState`) unchanged

3. **MODIFY:** `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`
   - Add side output for invalid events caught by `isValid()`: `public static final OutputTag<DeadLetterEvent> INVALID_ORDER_TAG = new OutputTag<>("invalid-order") {};`
   - In `isValid()` false path: emit `DeadLetterEvent` to side output

4. **MODIFY:** `flink-job/src/main/java/com/company/delayedordersms/DelayedOrderSmsJob.java`
   - After the `flatMap` call, capture side output:
     ```java
     SingleOutputStreamOperator<OrderState> orders = env
         .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source - Orders")
         .name("Read Orders")
         .flatMap(new OrderStateDeserializationFunction())
         .name("Parse Order State");
     
     DataStream<DeadLetterEvent> parseFailures = orders
         .getSideOutput(OrderStateDeserializationFunction.DEAD_LETTER_TAG);
     ```
   - After the `process` call, capture side output:
     ```java
     SingleOutputStreamOperator<SmsCommand> commands = orders
         .keyBy(OrderState::getOrderId)
         .process(new DelayedOrderProcessFunction())
         .name("Detect Delayed Orders");
     
     DataStream<DeadLetterEvent> invalidOrders = commands
         .getSideOutput(DelayedOrderProcessFunction.INVALID_ORDER_TAG);
     ```
   - Add Kafka sinks for both dead letter streams writing to topic `dead-letter-events`
   - Create a `DeadLetterEventSerializationSchema` (or reuse a generic JSON one) that serializes to JSON string

5. **NEW:** `flink-job/src/main/java/com/company/delayedordersms/serde/DeadLetterEventSerializationSchema.java`
   - Implements `KafkaRecordSerializationSchema<DeadLetterEvent>`
   - Serializes to JSON using Jackson `ObjectMapper` (WITH `JavaTimeModule` registered)

**AC:** Malformed JSON → `dead-letter-events`. Invalid order state (missing fields) → `dead-letter-events`. Each message includes `originalPayload`, `errorReason`, `occurredAt`. Valid events still work.

---

### TASK 4 OF 12: Add Custom Flink Metrics (HIGH)

**Files to modify:**
- `DelayedOrderProcessFunction.java`
- `OrderStateDeserializationFunction.java`

**Metrics to add:**

In `DelayedOrderProcessFunction.open()`:
```java
private transient Counter delayedOrdersDetected;
private transient Counter smsCommandsEmitted;
private transient Counter staleUpdatesIgnored;
private transient Counter invalidOrders;

@Override
public void open(Configuration parameters) {
    // ... existing state setup ...
    var metrics = getRuntimeContext().getMetricGroup();
    delayedOrdersDetected = metrics.counter("delayed_orders_detected");
    smsCommandsEmitted = metrics.counter("sms_commands_emitted");
    staleUpdatesIgnored = metrics.counter("stale_updates_ignored");
    invalidOrders = metrics.counter("invalid_messages");
}
```

In `OrderStateDeserializationFunction.open()`:
```java
private transient Counter parseErrors;

@Override
public void open(Configuration parameters) {
    var metrics = getRuntimeContext().getMetricGroup();
    parseErrors = metrics.counter("parse_errors");
}
```

**Increment rules:**
- `delayedOrdersDetected` — increment in `emitDelaySms()` (both overloads)
- `smsCommandsEmitted` — increment in `emitDelaySms()` (both overloads)
- `staleUpdatesIgnored` — increment in `isStaleUpdate()` when it returns true
- `invalidOrders` — increment in `processElement()` when `isValid()` returns false
- `parseErrors` — increment in `OrderStateDeserializationFunction` when JSON parse fails

**AC:** All 5 counters visible in Flink UI → Job → Metrics. Counters increment correctly during scenario runs.

---

### TASK 5 OF 12: Add State TTL (Time-To-Live) (MEDIUM)

**File:** `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`

**What to do:**
- In `open()`, after creating `ValueStateDescriptor`:
  ```java
  import org.apache.flink.api.common.state.StateTtlConfig;
  import org.apache.flink.api.common.time.Time;
  
  StateTtlConfig ttlConfig = StateTtlConfig
      .newBuilder(Time.days(7))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .cleanupFullSnapshot()
      .build();
  descriptor.enableTimeToLive(ttlConfig);
  ```
- Add `--state-ttl-days` to `JobConfig.java` (default 7)
- Read config value and use it in TTL config

**File:** `flink-job/src/main/java/com/company/delayedordersms/config/JobConfig.java`
- Add field: `private final int stateTtlDays;`
- Add CLI arg: `--state-ttl-days` with default 7
- Add getter: `public int stateTtlDays() { return stateTtlDays; }`

**AC:** State TTL configurable via `--state-ttl-days`. Terminal + SMS-emitted orders expire after TTL. Active orders not expired early.

---

### TASK 6 OF 12: Fix Hardcoded Paths + Create .gitignore + Remove Binaries (MEDIUM)

**File 1:** `run_scenario.py`
Replace the hardcoded `sys.path.insert(0, r'g:\Projects\...')` with:
```python
"""Shortcut to run order simulator scenarios."""
import sys
import os

_script_dir = os.path.dirname(os.path.abspath(__file__))
_simulator_src = os.path.join(_script_dir, 'simulator', 'src')
sys.path.insert(0, _simulator_src)

from order_simulator.main import main

if __name__ == "__main__":
    main()
```

**File 2:** `strip_jackson.py`
Replace entirely with version accepting CLI arguments:
```python
#!/usr/bin/env python3
"""Strip Jackson classes from fat jar to avoid classpath conflicts with Flink."""
import argparse
import zipfile
import os

def main():
    parser = argparse.ArgumentParser(description='Strip Jackson from fat JAR')
    parser.add_argument('src', help='Source JAR path')
    parser.add_argument('--output', '-o', help='Output JAR path', default=None)
    args = parser.parse_args()
    
    src = args.src
    dst = args.output if args.output else src.replace('.jar', '_clean.jar')
    
    JACKSON_PATTERNS = ['com/fasterxml', 'jackson-core', 'jackson-databind', 
                        'jackson-annotations', 'jackson-datatype']
    
    with zipfile.ZipFile(src, 'r') as zin:
        names = zin.namelist()
        stripped = 0
        with zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED) as zout:
            for n in names:
                if any(p in n for p in JACKSON_PATTERNS):
                    stripped += 1
                    continue
                zout.writestr(n, zin.read(n))
    
    print(f'Stripped {stripped}/{len(names)} Jackson entries -> {dst} ({os.path.getsize(dst)} bytes)')

if __name__ == '__main__':
    main()
```

**File 3:** Create `.gitignore` at project root:
```
# Build artifacts
*.jar
flink-job/target/

# Python
__pycache__/
*.pyc
.venv/
*.egg-info/

# IDE
.idea/
.vscode/
*.iml

# Temp files
config_*_temp.yaml

# Docker volumes / data
kafka-data/
flink-checkpoints/
flink-savepoints/

# OS
.DS_Store
Thumbs.db
```

**File cleanup — DELETE these files:**
- `job_7faa.jar`
- `job_7faa_nofat.jar`
- `job_clean.jar`
- `config_jm_temp.yaml`
- `config_tm_temp.yaml`

**AC:** `run_scenario.py` uses relative path. `strip_jackson.py` uses CLI args. `.gitignore` created. Binary artifacts removed from repo.

---

### TASK 7 OF 12: Complete JSON Schema Files (MEDIUM)

**File 1:** `schemas/order-events/order-state.schema.json`

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://company.com/schemas/order-state.schema.json",
  "title": "OrderState",
  "description": "Order state event published to the compacted Orders Kafka topic, keyed by orderId",
  "type": "object",
  "required": ["orderId", "customerId", "storeId", "status", "expectedDeliveryTime", "createdAt", "lastUpdatedAt", "eventTime", "stateLogs", "schemaVersion"],
  "properties": {
    "orderId": { "type": "string", "minLength": 1, "description": "Unique order identifier" },
    "customerId": { "type": "string", "minLength": 1, "description": "Customer identifier" },
    "storeId": { "type": "string", "minLength": 1, "description": "Store/branch identifier" },
    "status": {
      "type": "string",
      "enum": ["CREATED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED"],
      "description": "Current order lifecycle status"
    },
    "expectedDeliveryTime": { "type": "string", "format": "date-time", "description": "When the order is expected to be delivered (UTC ISO-8601)" },
    "createdAt": { "type": "string", "format": "date-time" },
    "lastUpdatedAt": { "type": "string", "format": "date-time", "description": "Timestamp of last state change, used for stale-update detection" },
    "eventTime": { "type": "string", "format": "date-time" },
    "stateLogs": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["status", "at"],
        "properties": {
          "status": { "type": "string", "enum": ["CREATED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED"] },
          "at": { "type": "string", "format": "date-time" }
        },
        "additionalProperties": false
      }
    },
    "schemaVersion": { "type": "integer", "const": 1 }
  },
  "additionalProperties": false
}
```

**File 2:** `schemas/sms-commands/send-delay-sms-command.schema.json`

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://company.com/schemas/send-delay-sms-command.schema.json",
  "title": "SendDelaySmsCommand",
  "description": "Command emitted when an order is delayed past its expected delivery time. Idempotent via commandId.",
  "type": "object",
  "required": ["commandId", "commandType", "orderId", "customerId", "storeId", "reason", "expectedDeliveryTime", "createdAt", "schemaVersion"],
  "properties": {
    "commandId": { "type": "string", "pattern": "^.+:DELAY_SMS$", "description": "Idempotent key: {orderId}:DELAY_SMS" },
    "commandType": { "type": "string", "const": "SEND_DELAY_SMS" },
    "orderId": { "type": "string", "minLength": 1 },
    "customerId": { "type": "string", "minLength": 1 },
    "storeId": { "type": "string", "minLength": 1 },
    "reason": { "type": "string", "const": "ORDER_DELAYED" },
    "expectedDeliveryTime": { "type": "string", "format": "date-time" },
    "createdAt": { "type": "string", "format": "date-time", "description": "When this command was generated" },
    "schemaVersion": { "type": "integer", "const": 1 }
  },
  "additionalProperties": false
}
```

**Validation:** Verify the example JSON in `schemas/README.md` passes both schemas. Use any JSON Schema validator (online or CLI).

**AC:** Both `.schema.json` files exist, valid Draft-07, example JSON from README validates successfully.

---

### TASK 8 OF 12: Add Unit Tests for Flink Job (MEDIUM)

**Check `flink-job/pom.xml` for test dependencies.** Add if missing:
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
    <classifier>tests</classifier>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>5.10.0</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.assertj</groupId>
    <artifactId>assertj-core</artifactId>
    <version>3.24.2</version>
    <scope>test</scope>
</dependency>
```

**Create test files:**

**File 1:** `flink-job/src/test/java/com/company/delayedordersms/processor/DelayedOrderProcessFunctionTest.java`

Use `KeyedOneInputStreamOperatorTestHarness` pattern. Test all 10 cases:

| # | Test Method Name | Input | Expected |
|---|-----------------|-------|----------|
| 1 | `shouldRegisterTimerWhenOrderHasFutureEta` | status=ACCEPTED, ETA=now+10min | Timer registered at ETA, no output |
| 2 | `shouldEmitSmsWhenEtaAlreadyPassed` | status=ACCEPTED, ETA=now-1min | SmsCommand emitted immediately |
| 3 | `shouldNotEmitSmsWhenOrderDelivered` | status=DELIVERED | No output, timer deleted |
| 4 | `shouldNotEmitSmsWhenOrderCancelled` | status=CANCELLED | No output, timer deleted |
| 5 | `shouldNotDuplicateSms` | Same order processed twice | Only one SmsCommand emitted |
| 6 | `shouldIgnoreStaleUpdate` | lastUpdatedAt older than state | No state change, no output |
| 7 | `shouldUpdateTimerWhenEtaChanges` | ETA updated to later time | Old timer deleted, new timer registered |
| 8 | `shouldSkipInvalidOrder` | orderId=null | No output, no crash |
| 9 | `shouldSkipTimerFireForTerminalOrder` | Timer fires after DELIVERED | No SmsCommand emitted |
| 10 | `shouldEmitSmsOnTimerFire` | Timer fires for active ACCEPTED order | SmsCommand emitted, delaySmsEmitted=true |

**File 2:** `flink-job/src/test/java/com/company/delayedordersms/serde/OrderStateDeserializationFunctionTest.java`
- Test valid JSON deserialization
- Test invalid JSON produces empty result (or side output after DLQ task)
- Test missing required fields

**File 3:** `flink-job/src/test/java/com/company/delayedordersms/config/JobConfigTest.java`
- Test default values
- Test CLI parsing with `--parallelism 4 --state-ttl-days 14`

**Run:** `mvn test -f flink-job/pom.xml`

**AC:** All 10 process function tests pass. `mvn test` green. Coverage > 80% on processor.

---

### TASK 9 OF 12: Add E2E Test Automation Script (MEDIUM)

**File:** `e2e-tests/run_e2e.py` (cross-platform Python script)

**Requirements:**
- Start: `docker compose up -d` (from project root)
- Poll until all services healthy (check `docker compose ps` - all "healthy" or "running")
- Build: `mvn clean package -f flink-job/pom.xml -DskipTests`
- Submit JAR to Flink via REST API: `POST http://localhost:8081/jars/upload` then `POST http://localhost:8081/jars/:jarid/run`
- Poll until job state = "RUNNING" via `GET http://localhost:8081/jobs`
- For each of 6 scenarios (delayed-orders, on-time-orders, cancelled-orders, duplicate-events, eta-updated-orders, mixed-orders):
  - Run: `python run_scenario.py <scenario> --orders-count 5`
  - Wait for expected time + buffer
  - Consume from `sms-commands` topic via Kafka CLI (or kafka-python)
  - Validate SMS count matches expected
- Report: pass/fail per scenario, exit code non-zero if any fail
- Support `--no-cleanup` flag to leave infrastructure running for debugging

**Expected SMS counts:**

| Scenario | Orders | Expected SMS |
|----------|--------|-------------|
| delayed-orders | 5 | 5 |
| on-time-orders | 5 | 0 |
| cancelled-orders | 5 | 0 |
| duplicate-events | 5 | 5 (NOT 10) |
| eta-updated-orders | 5 | 5 |
| mixed-orders | 5 | 1-2 |

**AC:** Single `python e2e-tests/run_e2e.py` command passes all 6 scenarios. Fails with non-zero exit on any failure.

---

### TASK 10 OF 12: Refactor Code Duplication in DelayedOrderProcessFunction (LOW)

**File:** `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`

**Problem:** Two `emitDelaySms()` methods (one takes `Context`, one takes `OnTimerContext`) with identical logic.

**What to do:**
- Remove both overloaded `emitDelaySms()` methods
- Add a single private method:
  ```java
  private void emitDelaySms(
          OrderDelayState current,
          long currentProcessingTimeMs,
          Collector<SmsCommand> out
  ) throws Exception {
      SmsCommand command = SmsCommand.delaySms(
              current,
              Instant.ofEpochMilli(currentProcessingTimeMs)
      );
      out.collect(command);
      current.setDelaySmsEmitted(true);
      current.setRegisteredTimerTime(null);
      orderState.update(current);
  }
  ```
- Update call sites in `processElement()`:
  ```java
  emitDelaySms(current, context.timerService().currentProcessingTime(), out);
  ```
- Update call sites in `onTimer()`:
  ```java
  emitDelaySms(current, context.timerService().currentProcessingTime(), out);
  ```

**AC:** Only one `emitDelaySms` method. `mvn test` passes. Behavior unchanged.

---

### TASK 11 OF 12: Remove Deprecated docker-compose.yml Version (LOW)

**File:** `docker-compose.yml`

**What to do:** Delete line 1: `version: "3.8"`

**AC:** `docker compose up` runs without deprecation warning. All services start correctly.

---

### TASK 12 OF 12: Verify & Fix Jackson JavaTimeModule Registration (LOW)

**Files to check/modify:**
- `flink-job/src/main/java/com/company/delayedordersms/serde/OrderStateDeserializationFunction.java`
- `flink-job/src/main/java/com/company/delayedordersms/serde/OrderStateParser.java`

**What to do:**
1. Read both files
2. Find where `ObjectMapper` is created
3. Add: `objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());`
4. Also add: `objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);`
5. Check `pom.xml` has `jackson-datatype-jsr310` dependency. If not, add:
   ```xml
   <dependency>
       <groupId>com.fasterxml.jackson.datatype</groupId>
       <artifactId>jackson-datatype-jsr310</artifactId>
       <version>${jackson.version}</version>
   </dependency>
   ```

**AC:** `ObjectMapper` has `JavaTimeModule` registered, ISO-8601 date strings parse correctly, unit test confirms round-trip.

---

## FINAL VERIFICATION

After completing all 12 tasks, run this checklist:

1. `mvn clean test -f flink-job/pom.xml` — ALL TESTS PASS
2. `docker compose up -d` — ALL SERVICES HEALTHY (no deprecation warnings)
3. Submit Flink JAR — JOB RUNNING
4. `python run_scenario.py delayed-orders --orders-count 5` — SIMULATOR RUNS
5. Check Kafka UI `http://localhost:8080` — SMS commands visible in `sms-commands` topic
6. Check Flink UI `http://localhost:8081` — Metrics visible, checkpoints succeeding
7. `docker compose down -v` — CLEAN TEARDOWN
8. `git status` — Only source files changed, no binaries, .gitignore in place

---

## COMMIT STRATEGY

Commit after EACH task with descriptive messages:
- `fix: use EmbeddedRocksDBStateBackend matching docker config`
- `docs: write ADR-0001 processing-time vs event-time`
- `docs: write ADR-0002 SMS idempotency strategy`
- `docs: write RFC for delayed order detection`
- `docs: write local runbook with setup/troubleshooting`
- `docs: write failure test runbook`
- `docs: write production runbook`
- `docs: write production proposal`
- `feat: add DeadLetterEvent model and side outputs`
- `feat: add DLQ sink for invalid events to dead-letter-events topic`
- `feat: add custom Flink metrics (delayed, sms, stale, invalid, parse)`
- `feat: add state TTL with --state-ttl-days config`
- `fix: use relative paths in run_scenario.py`
- `fix: accept CLI args in strip_jackson.py`
- `chore: add .gitignore, remove binary JARs and temp configs`
- `feat: add JSON Schema files for order-state and sms-command`
- `test: add DelayedOrderProcessFunction unit tests (10 cases)`
- `test: add deserialization and config unit tests`
- `test: add e2e automation script for all 6 scenarios`
- `refactor: deduplicate emitDelaySms in process function`
- `chore: remove deprecated version from docker-compose.yml`
- `fix: register JavaTimeModule on ObjectMapper for Instant deserialization`