# Simulator Scenarios

This directory contains scenario definitions used by the Order Event Simulator.

The simulator publishes order state snapshots to the compacted Kafka topic:

```
Orders
```

Each Kafka message must be keyed by:

```
orderId
```

Each message value must contain the complete latest order state.

## Important Rule

The simulator must not publish partial update events.

Correct:

```
{
  "orderId": "ord-001",
  "status": "ACCEPTED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "stateLogs": [...]
}
```

Incorrect:

```
{
  "eventType": "ORDER_ACCEPTED"
}
```

## Supported Scenarios

| File | Purpose |
|---|---|
| `on-time-orders.json` | Orders delivered before expected delivery time |
| `delayed-orders.json` | Orders not delivered or cancelled before expected delivery time |
| `cancelled-orders.json` | Orders cancelled before expected delivery time |
| `eta-updated-orders.json` | Orders with updated expected delivery time |
| `duplicate-events.json` | Duplicate order state messages |
| `out-of-order-updates.json` | Order snapshots published in unexpected order |
| `failure-recovery.json` | Scenario for Flink recovery and timer testing |
| `mixed-orders.json` | Mix of all supported scenarios |

## Dynamic Values

Scenario files may use dynamic time expressions:

```
now
now+10s
now+60s
now+180s
```

The simulator must resolve them to real ISO-8601 timestamps before publishing to Kafka.

## Template Variables

Scenario files may use template variables:

```
{{orderId}}
{{customerId}}
{{storeId}}
```

The simulator must replace these values for each generated order.

## Order Count

The number of orders should be configurable.

Default:

```
ORDERS_COUNT=100
```

Example:

```
ORDERS_COUNT=100 SCENARIO=mixed make simulate
```