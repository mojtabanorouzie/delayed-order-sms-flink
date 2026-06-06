# Dead-Letter Queue Runbook

Investigation and replay procedures for events that land on the `dead-letter-events` topic.

---

## What routes to DLQ

Every job routes two categories of invalid events to the DLQ:

| Category | Source | Example |
|----------|--------|---------|
| **Parse failures** | `OrderStateDeserializationFunction` | Malformed JSON, missing required fields, type mismatches |
| **Invalid orders** | Each processor's `INVALID_ORDER_TAG` | Null `orderId`, null `status` |
| **Invalid weather** | `WeatherDataDeserializationFunction` | Malformed weather JSON |

Each `DeadLetterEvent` record contains:
- `eventId` — unique ID for this DLQ record
- `sourceTopicName` — which Kafka topic the original event came from
- `rawPayload` — the original raw bytes, as a string
- `errorReason` — human-readable description of why it was rejected
- `detectedAt` — when the DLQ record was created

---

## How to inspect DLQ events

```bash
# See all DLQ events (up to 50)
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dead-letter-events \
  --from-beginning \
  --max-messages 50 \
  --timeout-ms 10000

# Pretty-print with jq (if installed)
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dead-letter-events \
  --from-beginning \
  --timeout-ms 5000 | while IFS= read -r line; do echo "$line" | python -m json.tool; done
```

In Kafka UI (`http://localhost:8080`):

1. Go to **Topics** → `dead-letter-events`
2. Click **Messages**
3. Filter by key or search by payload content

---

## Diagnosing common DLQ causes

### Parse failure

**Symptom:** `errorReason` contains "Failed to deserialize" or "Unrecognized field"

```json
{
  "errorReason": "Failed to deserialize OrderState: Unrecognized field 'deliveryZone'",
  "sourceTopicName": "Orders",
  "rawPayload": "{\"orderId\":\"abc\",\"deliveryZone\":\"z1\"}"
}
```

**Diagnosis:** The producer schema changed and added a field the consumer doesn't know about. Check whether `@JsonIgnoreProperties(ignoreUnknown = true)` is present on the model class. If not, add it — unknown fields should not cause DLQ events.

If the payload is truly malformed (not valid JSON), trace the upstream producer for the bug.

### Null orderId

**Symptom:** `errorReason` is "orderId is null or blank"

```json
{
  "errorReason": "orderId is null or blank",
  "sourceTopicName": "Orders"
}
```

**Diagnosis:** The upstream producer emitted an event without setting `orderId`. This is always a producer bug. Check the simulator scenario file or the upstream order service.

### Null status

**Symptom:** `errorReason` is "status is null"

**Diagnosis:** Same as above — producer omitted the `status` field. Check for schema versioning mismatches.

---

## Replay procedure

When a bug is fixed and you want to reprocess events that landed on DLQ:

### 1. Identify the events to replay

```bash
# Save DLQ events to a file for inspection
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic dead-letter-events \
  --from-beginning \
  --timeout-ms 5000 > /tmp/dlq-events.json

# Count events per error reason
cat /tmp/dlq-events.json | python -c "
import sys, json, collections
counts = collections.Counter()
for line in sys.stdin:
    try:
        e = json.loads(line)
        counts[e.get('errorReason','?')] += 1
    except: pass
for reason, count in counts.most_common():
    print(f'{count:4d}  {reason}')
"
```

### 2. Extract and repair raw payloads

For parse failures where the fix is adding `@JsonIgnoreProperties`, the raw payloads are already valid and just need to be re-published to the source topic:

```bash
# Extract rawPayload from DLQ events and re-publish to Orders topic
cat /tmp/dlq-events.json | python -c "
import sys, json
for line in sys.stdin:
    try:
        e = json.loads(line)
        if e.get('sourceTopicName') == 'Orders':
            print(e['rawPayload'])
    except: pass
" | docker exec -i kafka kafka-console-producer \
    --bootstrap-server kafka:29092 \
    --topic Orders
```

For structural bugs (null orderId), the payload cannot be replayed as-is — the upstream system must re-emit correct events.

### 3. Verify the replayed events are processed

After replay, monitor the output topics:

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic sms-commands \
  --from-beginning --timeout-ms 10000
```

Check that the DLQ is no longer growing:

```bash
# Watch DLQ offset — should stop increasing
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 \
  --describe --group delayed-order-sms-flink
```

---

## Alerting thresholds

In production, alert when DLQ message count exceeds:
- **5 events / minute** — investigate immediately (likely a schema change or producer bug)
- **1 event / hour** — acceptable background noise; review weekly

DLQ events should be zero during normal steady-state operation. Any DLQ event during an e2e test run is a test failure.

---

## DLQ topic configuration

```
Topic:      dead-letter-events
Partitions: 3
Cleanup:    delete
Retention:  30 days
```

30-day retention is intentional — it provides a replay window without consuming indefinite storage.
