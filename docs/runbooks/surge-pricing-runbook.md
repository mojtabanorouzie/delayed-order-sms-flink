# Surge Pricing Runbook

Operational procedures for the Dynamic Surge Pricing job (Job 5: `SurgePricingJob`).

---

## How surge pricing works

The job maintains a demand window per zone. Every `windowSizeSeconds` (default 60), it fires a timer and:

1. Computes `demandFactor = atRiskOrderCount / totalOrdersInWindow`
2. Gets `weatherFactor` from the latest weather state for that zone
3. Computes `combined = min(1.0 + demandFactor × demandWeight × weatherFactor, maxMultiplier)`
4. If `combined >= surgeThreshold` AND the multiplier changed by at least `changeThreshold` — emits a `SurgePricingSignal`

**Weather factors:** CLEAR=0.95, CLOUDY=1.00, RAIN=1.20, SNOW=1.40, UNKNOWN=1.00

**At-risk order:** `ETA remaining > atRiskThresholdMinutes` (default 25 min)

---

## Monitoring surge signals in production

```bash
# Tail live surge signals
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic surge-pricing-signals \
  --timeout-ms 60000

# Example output:
# {"signalId":"zone-downtown:1704067260000","zoneId":"zone-downtown",
#  "surgeMultiplier":1.72,"demandFactor":0.8,"weatherFactor":1.2,
#  "weatherCondition":"RAIN","ordersInWindow":5,"atRiskOrderCount":4,
#  "signalType":"SURGE_PRICING","schemaVersion":1}
```

Flink UI metrics to watch (under Job 5 → Task Manager → Metrics):
- `surge_signals_emitted` — total signals emitted (should increase during bad weather / high demand)
- `surge_weather_updates` — weather state updates received (should track weather topic frequency)
- `invalid_messages` — events routed to DLQ (should be zero)

---

## Injecting weather data manually

The `weather-data` topic is compacted, keyed by `region`. Inject a weather update:

```bash
# RAIN in zone-downtown
docker exec kafka bash -c \
  "echo '{\"region\":\"zone-downtown\",\"condition\":\"RAIN\",\"temperature\":12.0,\"windSpeed\":20.0,\"timestamp\":null,\"schemaVersion\":1}' \
   | kafka-console-producer --bootstrap-server kafka:29092 --topic weather-data \
     --property parse.key=true --property key.separator=, \
     --property key=zone-downtown"

# SNOW
docker exec kafka bash -c \
  "echo '{\"region\":\"zone-downtown\",\"condition\":\"SNOW\",\"temperature\":-2.0,\"windSpeed\":35.0,\"timestamp\":null,\"schemaVersion\":1}' \
   | kafka-console-producer --bootstrap-server kafka:29092 --topic weather-data"

# Back to CLEAR (will reduce surge multiplier by 0.95 factor)
docker exec kafka bash -c \
  "echo '{\"region\":\"zone-downtown\",\"condition\":\"CLEAR\",\"temperature\":22.0,\"windSpeed\":5.0,\"timestamp\":null,\"schemaVersion\":1}' \
   | kafka-console-producer --bootstrap-server kafka:29092 --topic weather-data"
```

**Important:** The job uses `OffsetsInitializer.latest()` for the weather topic. Weather events published before the job started are not consumed. Always inject weather after the job is running.

---

## Tuning parameters

All parameters can be changed by cancelling and resubmitting the job with different flags. State is restored from the latest checkpoint.

### When surge fires too aggressively

**Symptom:** Surge signals for zones with low actual delay risk.

| Parameter | Default | Increase to reduce sensitivity |
|---|---|---|
| `--surge.threshold` | 1.15 | Raise to 1.25 or 1.30 |
| `--at.risk.threshold.minutes` | 25 | Raise to 35 (fewer orders qualify as at-risk) |
| `--demand.weight` | 0.5 | Lower to 0.3 (demand component has less impact) |
| `--change.threshold` | 0.05 | Raise to 0.10 (suppress more near-duplicate signals) |

### When surge is not firing when expected

**Symptom:** No signals during known bad-weather / high-demand conditions.

| Parameter | Default | Change to increase sensitivity |
|---|---|---|
| `--surge.threshold` | 1.15 | Lower to 1.05 |
| `--at.risk.threshold.minutes` | 25 | Lower to 15 (more orders qualify as at-risk) |
| `--demand.weight` | 0.5 | Raise to 0.7 |
| `--window.size.seconds` | 60 | Lower to 30 (faster evaluation, more signals) |
| `--change.threshold` | 0.05 | Lower to 0.01 |

### When signals are too chatty

**Symptom:** Multiple signals per window with near-identical multipliers.

Raise `--change.threshold` to 0.10 or 0.15. This means a signal is only emitted when the multiplier changes by at least 10–15% relative to the last emitted value.

### Hard cap on multiplier

`--max.multiplier` (default 3.0) is a hard cap. Regardless of demand and weather, no signal will have a `surgeMultiplier` above this value. Adjust based on business policy.

---

## Resubmitting after a parameter change

```bash
# 1. Get the current job ID
docker exec flink-jobmanager flink list

# 2. Take a savepoint before cancelling (preserves state)
docker exec flink-jobmanager flink savepoint <JOB_ID> \
  file:///opt/flink/checkpoints/savepoints

# 3. Cancel the job
docker exec flink-jobmanager flink cancel <JOB_ID>

# 4. Resubmit with new parameters, restoring from savepoint
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.SurgePricingJob \
  /tmp/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --surge.signals.topic surge-pricing-signals \
  --consumer.group.id surge-pricing-flink \
  --checkpoint.storage.path file:///opt/flink/checkpoints \
  --window.size.seconds 60 \
  --surge.threshold 1.20 \
  --demand.weight 0.5 \
  -s file:///opt/flink/checkpoints/savepoints/<SAVEPOINT_DIR>
```

---

## Disabling surge pricing

To temporarily disable surge signals without stopping the job, set `--surge.enabled false`. The job continues running and consuming Kafka offsets, but emits no signals. Re-enable by resubmitting with `--surge.enabled true`.

```bash
docker exec flink-jobmanager flink cancel <JOB_ID>
docker exec flink-jobmanager flink run -d \
  -c com.company.delayedordersms.SurgePricingJob \
  /tmp/delayed-order-sms-flink-job.jar \
  ... \
  --surge.enabled false
```

---

## Expected multiplier ranges

| Scenario | demandFactor | Weather | Multiplier |
|---|---|---|---|
| Low demand, clear weather | 0.2 | CLEAR (0.95) | 1.0 + 0.2 × 0.5 × 0.95 = **1.095** → below threshold, no signal |
| High demand, no weather | 0.8 | UNKNOWN (1.0) | 1.0 + 0.8 × 0.5 × 1.0 = **1.40** |
| High demand, rain | 0.8 | RAIN (1.2) | 1.0 + 0.8 × 0.5 × 1.2 = **1.48** |
| All at-risk, snow | 1.0 | SNOW (1.4) | 1.0 + 1.0 × 0.5 × 1.4 = **1.70** |
| Max multiplier hit | 1.0 | SNOW (1.4) + weight 1.0 | capped at **3.0** |

---

## Troubleshooting

**No signals at all:**
1. Confirm the job is RUNNING: `docker exec flink-jobmanager flink list`
2. Confirm weather was injected after job started
3. Check whether `surge.enabled=true`
4. Reduce `surge.threshold` to 1.01 to test basic signal emission

**Signals for wrong zones:**
Weather `region` must exactly match the order `zoneId`. Check the scenario files and weather injection commands for typos.

**DLQ events for weather:**
Check `dead-letter-events` for `WeatherData` parse errors. The weather JSON must match the `WeatherData` POJO schema: `region`, `condition`, `temperature`, `windSpeed`, `schemaVersion`.
