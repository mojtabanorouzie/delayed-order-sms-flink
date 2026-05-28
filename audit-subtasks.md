# Delayed Order SMS Flink – Project Audit & Fix Subtasks

> **Generated:** 2026-05-28  
> **Audit Scope:** Full project review (READMEs, Flink jobs, simulator, schemas, docs, configs, build scripts)  
> **Severity Levels:** 🔴 Critical | 🟠 High | 🟡 Medium | 🟢 Low

---

## Overview

| Area | Files Reviewed | Status |
|------|---------------|--------|
| Root configs & scripts | `docker-compose.yml`, `run_scenario.py`, `repack.py`, `strip_jackson.py`, `config_*_temp.yaml`, `*.jar` | Needs fixes |
| Flink job | `DelayedOrderSmsJob.java`, `DelayedOrderProcessFunction.java`, models, serde, config | Needs fixes |
| Simulator | `main.py`, config, runner, kafka_producer, scenario_loader, template_renderer, time_utils | OK (minor fix) |
| Schemas | `schemas/README.md`, schema directories | Needs verification |
| E2E tests | `e2e-tests/README.md` | Manual only, needs automation |
| Docs | ADR, RFC, runbooks | ALL EMPTY |
| Proposal | `proposal/production-proposal.md` | EMPTY |

---

## Subtask 1: Fix Flink Job State Backend Mismatch 🔴 CRITICAL

**Problem:** `DelayedOrderSmsJob.java` line 30 uses `HashMapStateBackend` (in-memory), but `docker-compose.yml` configures `state.backend.type: rocksdb`. This mismatch means checkpoints use different backends depending on whether the config is read from `flink-conf.yaml` or the code. HashMapStateBackend is limited by JVM heap and does not support incremental checkpoints.

**Files to change:**
- `flink-job/src/main/java/com/company/delayedordersms/DelayedOrderSmsJob.java`

**Required change:**
```java
// REMOVE:
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
// Line 30: env.setStateBackend(new HashMapStateBackend());

// REPLACE WITH:
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
// Line 30: env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
```

**Acceptance criteria:**
- [ ] `EmbeddedRocksDBStateBackend` used in code, matching `docker-compose.yml`
- [ ] Job builds successfully with `mvn clean package`
- [ ] Checkpoints visible in Flink UI after job submission

---

## Subtask 2: Write ALL Empty Documentation Files 🔴 CRITICAL

**Problem:** The following files exist but contain ZERO content (1 byte empty placeholders):
- `docs/adr/0001-processing-time-vs-event-time.md`
- `docs/adr/0002-sms-idempotency-strategy.md`
- `docs/rfc/delayed-order-detection-rfc.md`
- `docs/runbooks/local-runbook.md`
- `docs/runbooks/failure-test-runbook.md`
- `docs/runbooks/production-runbook.md`
- `proposal/production-proposal.md`

**Files to create/write:**
- `docs/adr/0001-processing-time-vs-event-time.md`
- `docs/adr/0002-sms-idempotency-strategy.md`
- `docs/rfc/delayed-order-detection-rfc.md`
- `docs/runbooks/local-runbook.md`
- `docs/runbooks/failure-test-runbook.md`
- `docs/runbooks/production-runbook.md`
- `proposal/production-proposal.md`

**Content requirements for each:**

### 2a. ADR-0001: Processing Time vs Event Time
- Document the decision to use processing-time timers
- Explain why event-time watermarking was NOT chosen
- Trade-offs: simplicity vs determinism, late data handling
- Impact on replay/debugging scenarios
- Reference: Flink README already documents "Time Semantics" section — extract and formalize

### 2b. ADR-0002: SMS Idempotency Strategy
- Document the `commandId = orderId + ":DELAY_SMS"` deterministic ID scheme
- Explain the `delaySmsEmitted` boolean in Flink state
- Trade-offs: exactly-once vs at-least-once semantics
- Impact on downstream SMS gateway deduplication
- Kafka compaction/key strategy for `sms-commands` topic

### 2c. RFC: Delayed Order Detection
- Full pipeline architecture diagram
- Input/output topic contracts
- State model and timer semantics
- Scalability considerations (key distribution, parallelism)
- Failure modes and recovery behavior
- Monitoring and observability requirements

### 2d. Local Runbook
- Prerequisites (Docker, Java, Maven, Python)
- Step-by-step local setup
- Common troubleshooting steps (extract from e2e-tests/README.md)
- Health check commands
- Tear-down instructions

### 2e. Failure Test Runbook
- Step-by-step failure injection procedure
- How to cancel/restart Flink job
- How to verify checkpoint recovery
- Expected behavior after recovery
- How to validate no duplicate SMS

### 2f. Production Runbook
- Deployment architecture
- Kubernetes/Flink operator configuration
- Monitoring setup (metrics, alerts)
- Scaling guidance
- Backup/restore procedures
- Incident response playbook

### 2g. Production Proposal
- Business case
- Architecture overview
- Infrastructure requirements
- Cost estimates
- Migration plan
- Risk assessment

**Acceptance criteria:**
- [ ] All 7 files contain substantive, non-placeholder content
- [ ] ADRs follow standard ADR format (Title, Status, Context, Decision, Consequences)
- [ ] RFC includes architecture diagrams (ASCII or Mermaid)
- [ ] Runbooks include copy-paste-ready CLI commands
- [ ] Production proposal addresses all listed sections

---

## Subtask 3: Add DLQ (Dead Letter Queue) for Invalid Events 🟠 HIGH

**Problem:** `DelayedOrderProcessFunction.isValid()` returns false for invalid events, but those events are silently dropped. The `dead-letter-events` topic is already provisioned in `docker-compose.yml`, and the Flink README lists this as a planned improvement. Invalid events include: missing `orderId`, missing `status`, missing `expectedDeliveryTime`, missing `lastUpdatedAt`, and JSON parse failures from `OrderStateDeserializationFunction`.

**Files to change:**
- `flink-job/src/main/java/com/company/delayedordersms/DelayedOrderSmsJob.java`
- `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`
- `flink-job/src/main/java/com/company/delayedordersms/serde/OrderStateDeserializationFunction.java`
- NEW: `flink-job/src/main/java/com/company/delayedordersms/model/DeadLetterEvent.java` (if needed)

**Required changes:**
1. Create a `DeadLetterEvent` model class containing: original raw message, error reason, timestamp, topic of origin
2. Modify `OrderStateDeserializationFunction` to emit invalid events to a side output (using `OutputTag<DeadLetterEvent>`)
3. Add a Kafka sink for the `dead-letter-events` topic writing from the side output
4. Add the DLQ sink registration in `DelayedOrderSmsJob.main()`

**Acceptance criteria:**
- [ ] Invalid JSON messages appear in `dead-letter-events` topic
- [ ] Invalid order states (missing required fields) appear in `dead-letter-events` topic
- [ ] Each DLQ message includes: original raw payload, error reason, timestamp
- [ ] Smoke test: send malformed JSON to `Orders`, verify it appears in `dead-letter-events`
- [ ] Valid events continue to be processed normally

---

## Subtask 4: Add Custom Flink Metrics 🟠 HIGH

**Problem:** The Flink README lists custom metrics as a planned improvement. Without metrics, operational visibility is zero.

**Files to change:**
- `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`
- `flink-job/src/main/java/com/company/delayedordersms/serde/OrderStateDeserializationFunction.java`

**Required metrics:**

| Metric Name | Type | Description |
|-------------|------|-------------|
| `delayed_orders_detected` | Counter | Total delayed orders that triggered SMS |
| `sms_commands_emitted` | Counter | Total SEND_DELAY_SMS commands emitted |
| `stale_updates_ignored` | Counter | Incoming messages dropped as stale |
| `invalid_messages` | Counter | Messages failing validation/parsing |
| `active_timers` | Gauge | Number of currently registered processing-time timers |
| `active_orders` | Gauge | Number of active (non-terminal) orders in state |

**Implementation:**
- Use Flink `Counter` and `Gauge` via `getRuntimeContext().getMetricGroup()`
- The `active_timers` and `active_orders` gauge should use `RichFunction`/`AbstractRichFunction` idioms or be reported from a separate periodic reporter

**Acceptance criteria:**
- [ ] All 6 metrics exposed in Flink UI → Job → Metrics
- [ ] Metrics increment correctly during scenario runs
- [ ] `stale_updates_ignored` increments during `out-of-order-updates` scenario
- [ ] `invalid_messages` increments when malformed JSON is sent

---

## Subtask 5: Add State TTL (Time-To-Live) 🟡 MEDIUM

**Problem:** Flink state for terminal orders (DELIVERED, CANCELLED) and orders that already emitted SMS is kept indefinitely. Over time, this causes unbounded state growth. The Flink README lists this as a planned improvement.

**Files to change:**
- `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`
- `flink-job/src/main/java/com/company/delayedordersms/config/JobConfig.java`

**Required changes:**
1. Configure TTL on the `ValueStateDescriptor` in `open()`:
   ```java
   StateTtlConfig ttlConfig = StateTtlConfig
       .newBuilder(Time.days(7))  // configurable
       .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
       .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
       .cleanupFullSnapshot()
       .build();
   descriptor.enableTimeToLive(ttlConfig);
   ```
2. Add `--state-ttl-days` CLI argument with default `7`
3. After SMS emitted + TTL window, state should be cleaned up on next checkpoint/snapshot
4. Ensure terminal orders are NOT cleaned up before the SMS emission window passes

**Acceptance criteria:**
- [ ] State TTL configurable via CLI argument `--state-ttl-days`
- [ ] State for terminal + SMS-emitted orders expires after TTL
- [ ] Active orders (no SMS emitted yet) are NOT expired early
- [ ] Checkpoint size does not grow unbounded over time

---

## Subtask 6: Fix Hardcoded Absolute Paths & Remove Binary Artifacts 🟡 MEDIUM

**Problem A:** `run_scenario.py` line 3 has a hardcoded absolute Windows path:
```python
sys.path.insert(0, r'g:\Projects\PersonalWebsite\delayed-order-sms-flink\simulator\src')
```
This breaks the script on any other machine or operating system.

**Problem B:** `strip_jackson.py` lines 7-8 have hardcoded absolute Windows paths:
```python
SRC = r'g:\Projects\PersonalWebsite\delayed-order-sms-flink\job_7faa.jar'
DST = r'g:\Projects\PersonalWebsite\delayed-order-sms-flink\job_clean.jar'
```

**Problem C:** Binary JAR files committed to the repository:
- `job_7faa.jar`
- `job_7faa_nofat.jar`
- `job_clean.jar`

**Problem D:** Temp config files at root:
- `config_jm_temp.yaml`
- `config_tm_temp.yaml`

**Files to change:**
- `run_scenario.py` — use relative path
- `strip_jackson.py` — use command-line arguments or relative paths
- Remove: `job_7faa.jar`, `job_7faa_nofat.jar`, `job_clean.jar`
- Remove or `.gitignore`: `config_jm_temp.yaml`, `config_tm_temp.yaml`
- Create/update: `.gitignore`

**Required changes:**

### 6a. Fix `run_scenario.py`
```python
"""Shortcut to run order simulator scenarios."""
import sys
import os

# Use path relative to this script's location
_script_dir = os.path.dirname(os.path.abspath(__file__))
_simulator_src = os.path.join(_script_dir, 'simulator', 'src')
sys.path.insert(0, _simulator_src)

from order_simulator.main import main

if __name__ == "__main__":
    main()
```

### 6b. Fix `strip_jackson.py`
Convert to accept command-line arguments:
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

### 6c. Create `.gitignore`
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

### 6d. Remove binary artifacts
- Delete `job_7faa.jar`, `job_7faa_nofat.jar`, `job_clean.jar`
- Delete `config_jm_temp.yaml`, `config_tm_temp.yaml`

**Acceptance criteria:**
- [ ] `run_scenario.py` uses `os.path.dirname(__file__)` relative path
- [ ] `strip_jackson.py` accepts CLI arguments, no hardcoded paths
- [ ] `.gitignore` created and committed
- [ ] Binary JARs removed from repo (`git rm`)
- [ ] Temp config files removed or gitignored

---

## Subtask 7: Complete JSON Schema Files 🟡 MEDIUM

**Problem:** The `schemas/` directory has README files but may not have actual JSON Schema files. The structure shows `schemas/order-events/` and `schemas/sms-commands/` directories but JSON schema files need to be verified and potentially created.

**Files to create/verify:**
- `schemas/order-events/order-state.schema.json` — JSON Schema Draft-07
- `schemas/sms-commands/send-delay-sms-command.schema.json` — JSON Schema Draft-07

**Content requirements:**

### 7a. Order State Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://company.com/schemas/order-state.schema.json",
  "title": "OrderState",
  "type": "object",
  "required": ["orderId", "customerId", "storeId", "status", "expectedDeliveryTime", "createdAt", "lastUpdatedAt", "eventTime", "stateLogs", "schemaVersion"],
  "properties": {
    "orderId": { "type": "string", "minLength": 1 },
    "customerId": { "type": "string", "minLength": 1 },
    "storeId": { "type": "string", "minLength": 1 },
    "status": { "type": "string", "enum": ["CREATED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED"] },
    "expectedDeliveryTime": { "type": "string", "format": "date-time" },
    "createdAt": { "type": "string", "format": "date-time" },
    "lastUpdatedAt": { "type": "string", "format": "date-time" },
    "eventTime": { "type": "string", "format": "date-time" },
    "stateLogs": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["status", "at"],
        "properties": {
          "status": { "type": "string", "enum": ["CREATED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED"] },
          "at": { "type": "string", "format": "date-time" }
        }
      }
    },
    "schemaVersion": { "type": "integer", "const": 1 }
  }
}
```

### 7b. SMS Command Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "https://company.com/schemas/send-delay-sms-command.schema.json",
  "title": "SendDelaySmsCommand",
  "type": "object",
  "required": ["commandId", "commandType", "orderId", "customerId", "storeId", "reason", "expectedDeliveryTime", "createdAt", "schemaVersion"],
  "properties": {
    "commandId": { "type": "string", "minLength": 1 },
    "commandType": { "type": "string", "const": "SEND_DELAY_SMS" },
    "orderId": { "type": "string", "minLength": 1 },
    "customerId": { "type": "string", "minLength": 1 },
    "storeId": { "type": "string", "minLength": 1 },
    "reason": { "type": "string", "const": "ORDER_DELAYED" },
    "expectedDeliveryTime": { "type": "string", "format": "date-time" },
    "createdAt": { "type": "string", "format": "date-time" },
    "schemaVersion": { "type": "integer", "const": 1 }
  }
}
```

**Acceptance criteria:**
- [ ] `order-state.schema.json` exists and is valid JSON Schema Draft-07
- [ ] `send-delay-sms-command.schema.json` exists and is valid JSON Schema Draft-07
- [ ] Example JSON from README passes schema validation
- [ ] Both schemas include `$id` and `$schema` fields

---

## Subtask 8: Add Unit Tests for Flink Job 🟡 MEDIUM

**Problem:** `flink-job/src/test/java/` exists but is empty. The Flink README acknowledges "Unit tests are minimal initially." Without tests, regression risk is high.

**Files to create:**
- `flink-job/src/test/java/com/company/delayedordersms/processor/DelayedOrderProcessFunctionTest.java`
- `flink-job/src/test/java/com/company/delayedordersms/serde/OrderStateDeserializationFunctionTest.java`
- `flink-job/src/test/java/com/company/delayedordersms/model/OrderStateTest.java`
- `flink-job/src/test/java/com/company/delayedordersms/config/JobConfigTest.java`

**Test cases required for DelayedOrderProcessFunction:**

| Test Case | Input | Expected Behavior |
|-----------|-------|-------------------|
| Active order with future ETA | status=ACCEPTED, ETA=future | Timer registered, no SMS emitted |
| Active order with past ETA | status=ACCEPTED, ETA=past | SMS emitted immediately |
| Delivered order | status=DELIVERED, ETA=future | Timer deleted, no SMS |
| Cancelled order | status=CANCELLED, ETA=future | Timer deleted, no SMS |
| Duplicate SMS prevention | Same order processed twice after delay | Only one SMS emitted |
| Stale update ignored | Incoming lastUpdatedAt < state lastUpdatedAt | No state change |
| ETA updated | ETA changed to later time | Old timer deleted, new timer registered |
| Invalid input (null fields) | Missing required fields | Event ignored, no crash |
| Timer fires for terminal order | Timer fires after order delivered | No SMS emitted |
| Timer fires correctly | Timer fires at ETA for active order | SMS emitted once |

**Test infrastructure:**
- Use Flink `KeyedOneInputStreamOperatorTestHarness` for process function tests
- Use JUnit 5 (Jupiter)
- Use AssertJ for assertions (already in pom.xml?)

**Dependencies to add to `pom.xml` if missing:**
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

**Acceptance criteria:**
- [ ] All 10 test cases pass with `mvn test`
- [ ] Test coverage > 80% for `DelayedOrderProcessFunction`
- [ ] `OrderStateDeserializationFunction` has tests for valid JSON, invalid JSON, and missing fields
- [ ] `JobConfig` has tests for CLI argument parsing with defaults

---

## Subtask 9: Add E2E Test Automation Script 🟡 MEDIUM

**Problem:** The `e2e-tests/` directory contains only a README with manual test steps. No automated e2e test script exists. This makes regression testing labor-intensive and error-prone.

**File to create:**
- `e2e-tests/run_e2e.sh` (Linux/Mac) or `e2e-tests/run_e2e.py` (cross-platform, recommended)

**Requirements for the automation script:**
1. Start Docker Compose infrastructure
2. Wait for all services healthy (poll `docker compose ps`)
3. Build Flink JAR (`mvn clean package -f flink-job/pom.xml`)
4. Submit Flink job
5. Wait for job RUNNING state
6. For each scenario:
   - Run simulator with `--orders-count 5`
   - Wait for processing (based on scenario timer delays + buffer)
   - Consume from `sms-commands` topic
   - Validate expected SMS count per scenario
7. Report pass/fail per scenario
8. Clean up (optional, controlled by `--no-cleanup` flag)

**Expected SMS count per scenario:**

| Scenario | Orders Count | Expected SMS Count |
|----------|-------------|-------------------|
| delayed-orders | 5 | 5 |
| on-time-orders | 5 | 0 |
| cancelled-orders | 5 | 0 |
| duplicate-events | 5 | 5 (not 10) |
| eta-updated-orders | 5 | 5 |
| mixed-orders | 5 | 1-2 |

**Acceptance criteria:**
- [ ] Single command runs all 6 scenarios and reports results
- [ ] Script validates SMS count per scenario
- [ ] Script fails with non-zero exit code if any scenario fails
- [ ] Script works on fresh `docker compose up` (no pre-existing state assumption)
- [ ] Script includes a `--no-cleanup` flag for debugging

---

## Subtask 10: Refactor `DelayedOrderProcessFunction` — Remove Code Duplication 🟢 LOW

**Problem:** `DelayedOrderProcessFunction.java` has two `emitDelaySms` methods (one for `Context`, one for `OnTimerContext`) with identical logic. This is code duplication.

**Files to change:**
- `flink-job/src/main/java/com/company/delayedordersms/processor/DelayedOrderProcessFunction.java`

**Required change:**
Extract common logic into a single private method:
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

Then call it from both `Context` and `OnTimerContext` paths:
```java
// In processElement (Context path):
emitDelaySms(current, context.timerService().currentProcessingTime(), out);

// In onTimer (OnTimerContext path):
emitDelaySms(current, context.timerService().currentProcessingTime(), out);
```

**Acceptance criteria:**
- [ ] Only one `emitDelaySms` method body exists
- [ ] All existing behavior preserved (verified by unit tests from Subtask 8)
- [ ] `mvn test` passes

---

## Subtask 11: Fix `docker-compose.yml` Version Warning 🟢 LOW

**Problem:** `docker-compose.yml` line 1 uses `version: "3.8"` which is deprecated since Docker Compose V2 (July 2023). This generates a warning on every run.

**Files to change:**
- `docker-compose.yml`

**Required change:**
Remove line 1 (`version: "3.8"`) entirely. The `version` top-level element is obsolete in Docker Compose V2.

**Acceptance criteria:**
- [ ] `docker compose up` runs without deprecation warning
- [ ] All services start correctly
- [ ] No behavioral change

---

## Subtask 12: Verify and Fix Jackson Date Serialization 🟢 LOW

**Problem:** `OrderState.java` uses `Instant` fields with Jackson's default deserialization. Jackson's default `ObjectMapper` does NOT include `JavaTimeModule`, which means `Instant` fields may fail to deserialize unless the `JavaTimeModule` is registered. If the Maven shade plugin pulls in `jackson-datatype-jsr310` but `JavaTimeModule` is not registered, date fields will silently fail or throw exceptions.

**Files to check/change:**
- `flink-job/src/main/java/com/company/delayedordersms/serde/OrderStateDeserializationFunction.java` or `OrderStateParser.java`
- `flink-job/pom.xml`

**Required investigation:**
1. Check if `OrderStateParser.java` registers `JavaTimeModule` on its `ObjectMapper`
2. If not, add: `objectMapper.registerModule(new JavaTimeModule());`
3. Verify `jackson-datatype-jsr310` is in `pom.xml` dependencies
4. Add a test that deserializes a full JSON order state with ISO-8601 timestamps

**Acceptance criteria:**
- [ ] `ObjectMapper` has `JavaTimeModule` registered
- [ ] `Instant` fields deserialize correctly from ISO-8601 strings
- [ ] Unit test verifies round-trip JSON parsing

---

## Execution Order (Recommended)

| Order | Subtask | Depends On | Estimated Effort |
|-------|---------|------------|-----------------|
| 1 | Subtask 6: Fix paths + create .gitignore | None | 30 min |
| 2 | Subtask 1: Fix State Backend | None | 30 min |
| 3 | Subtask 10: Refactor code duplication | None | 15 min |
| 4 | Subtask 12: Fix Jackson serialization | None | 30 min |
| 5 | Subtask 8: Add unit tests | 1, 3, 10, 12 | 2-3 hours |
| 6 | Subtask 3: Add DLQ | 1 | 1-2 hours |
| 7 | Subtask 4: Add custom metrics | 1, 3 | 1 hour |
| 8 | Subtask 5: Add state TTL | 3 | 1 hour |
| 9 | Subtask 7: Complete JSON schemas | None | 30 min |
| 10 | Subtask 9: E2E test automation | 1, 3, 6, 8 | 2-3 hours |
| 11 | Subtask 2: Write documentation | 1-10 (all context) | 3-4 hours |
| 12 | Subtask 11: Fix compose version | None | 5 min |

**Total estimated effort:** 12-17 hours (1-2 senior engineer days)

---

## Notes for Agents

1. **Run tests after each subtask.** Use `mvn test` for Flink job changes and `--dry-run` for simulator changes.
2. **Commit after each subtask** with a descriptive message like `fix: use RocksDBStateBackend to match docker config`
3. **Do NOT modify `docker-compose.yml` Kafka configurations** (replication factor, partitions, listeners) — those are correct for local dev.
4. **The `Orders` topic is compacted** — all test scenarios must respect this (key=orderId, value=full state).
5. **Processing-time timers are intentional** — do not convert to event-time. The ADR should document why this was chosen.
6. **All time values are UTC ISO-8601** in the format `2026-05-12T19:30:00Z`.