# Schemas

This directory contains JSON schemas used by the delayed order SMS Flink POC.

## Order State Schema

Path:

```text
schemas/order-events/order-state.schema.json
```

The source Kafka topic is:

```text
Orders
```

`Orders` is a compacted topic.

Each Kafka message must be keyed by:

```text
orderId
```

Each message value must contain the full latest order state.

The simulator and Flink job should use this schema as the contract for consumed order state messages.

## SMS Command Schema

Path:

```text
schemas/sms-commands/send-delay-sms-command.schema.json
```

The Flink job emits delay SMS commands to:

```text
sms-commands
```

Each command must include an idempotent `commandId`:

```text
commandId = orderId + ":DELAY_SMS"
```

Example:

```text
ord-123:DELAY_SMS
```

## Schema Version

Current schema version:

```text
1
```

Schema evolution should be handled explicitly before production usage.
```

---

# Example Valid `Order State`

```json
{
  "orderId": "ord-001",
  "customerId": "cus-001",
  "storeId": "store-001",
  "status": "ACCEPTED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T18:45:00Z",
  "lastUpdatedAt": "2026-05-12T18:45:10Z",
  "eventTime": "2026-05-12T18:45:10Z",
  "stateLogs": [
    {
      "status": "CREATED",
      "at": "2026-05-12T18:45:00Z"
    },
    {
      "status": "ACCEPTED",
      "at": "2026-05-12T18:45:10Z"
    }
  ],
  "schemaVersion": 1
}
```

---

# Example Valid `SEND_DELAY_SMS` Command

```json
{
  "commandId": "ord-001:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS",
  "orderId": "ord-001",
  "customerId": "cus-001",
  "storeId": "store-001",
  "reason": "ORDER_DELAYED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T19:31:00Z",
  "schemaVersion": 1
}
```