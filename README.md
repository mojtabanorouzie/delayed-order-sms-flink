# Delayed Order SMS Flink

A project-based Apache Flink POC for detecting delayed orders in real time and emitting SMS commands for customer care.

This repository is created as part of an internal Flink learning and design workshop.  
The goal is to build an end-to-end local streaming system that simulates company order events, processes them with Apache Flink, detects delayed orders, and produces idempotent SMS command events.

---

## Problem Statement

In our ordering platform, some customer orders may pass their expected delivery time without being delivered or cancelled.

We want to detect these delayed orders in near real time and trigger a customer care SMS command such as:

> "We are sorry for the delay. We are checking the issue with your order."

This project does **not** send SMS directly.  
Instead, it emits an idempotent command to a Kafka topic. A downstream notification service can consume that command and send the actual SMS.

---

## Goals

- Build a local end-to-end stream processing environment.
- Simulate realistic order events based on company event models.
- Process order events using Apache Flink.
- Detect orders that pass their expected delivery time.
- Emit a `SEND_DELAY_SMS` command for delayed orders.
- Avoid duplicate SMS commands.
- Test the pipeline with different scenarios.
- Practice Flink concepts such as:
  - Kafka Source/Sink
  - Keyed State
  - Processing Time Timers
  - Checkpointing
  - Failure Recovery
  - Savepoints
  - Idempotency
- Prepare a production proposal for implementing this system in the real environment.

---

## Non-Goals

- Sending real SMS messages.
- Connecting directly to external SMS providers from Flink.
- Replacing the notification service.
- Building a production-ready system in the first phase.
- Handling all possible order lifecycle complexities from day one.

---

## High-Level Architecture

```text
Order Event Simulator
        |
        v
Kafka Topic: order-events
        |
        v
Apache Flink Job
        |
        v
Kafka Topic: sms-commands
        |
        v
Notification Service / SMS Orchestrator
        |
        v
SMS Provider
```
In this POC, the Notification Service and SMS Provider are not implemented.
The Flink job only produces SEND_DELAY_SMS commands into Kafka.

## Main Use Case
When an order is created, it has an expectedDeliveryTime.

If the order is not delivered or cancelled before that time, the Flink job emits a delay SMS command.

Example:
```
ORDER_CREATED at 18:00
expectedDeliveryTime = 18:45
```
If no ORDER_DELIVERED or ORDER_CANCELLED event is received before 18:45,
emit SEND_DELAY_SMS command.

## Order Events
The input stream contains order lifecycle events.

Supported event types:
```
ORDER_CREATED
ORDER_ACCEPTED
ORDER_PICKED_UP
ORDER_DELIVERED
ORDER_CANCELLED
ORDER_ETA_UPDATED
```
Example ORDER_CREATED event:
```
{
  "eventId": "evt-1001",
  "eventType": "ORDER_CREATED",
  "orderId": "ord-123",
  "customerId": "cus-456",
  "storeId": "store-789",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "eventTime": "2026-05-12T18:45:00Z",
  "occurredAt": "2026-05-12T18:45:02Z",
  "schemaVersion": 1
}
```
Example ORDER_DELIVERED event:
```
{
  "eventId": "evt-1002",
  "eventType": "ORDER_DELIVERED",
  "orderId": "ord-123",
  "eventTime": "2026-05-12T19:20:00Z",
  "occurredAt": "2026-05-12T19:20:05Z",
  "schemaVersion": 1
}
```
## Output Command
When an order is detected as delayed, the Flink job emits a command to the sms-commands topic.

Example SEND_DELAY_SMS command:
```
{
  "commandId": "ord-123:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS",
  "orderId": "ord-123",
  "customerId": "cus-456",
  "storeId": "store-789",
  "reason": "ORDER_DELAYED",
  "expectedDeliveryTime": "2026-05-12T19:30:00Z",
  "createdAt": "2026-05-12T19:31:00Z",
  "schemaVersion": 1
}
```
The commandId must be idempotent:
```
commandId = orderId + ":DELAY_SMS"
```
This helps downstream services avoid sending duplicate SMS messages.

## Time Semantics
For this POC, the delay detection trigger is based on Processing Time.

### Reason:

The business question is:
"Has the real current time passed the expected delivery time?"
Therefore, the timer should fire based on wall-clock time.
However, input events still include eventTime for observability, debugging, ordering analysis, and future event-time processing needs.

## Local Development Environment
The local environment should include:
```
Apache Kafka
Apache Flink JobManager
Apache Flink TaskManager
Kafka UI / Redpanda Console
Order Event Simulator
Flink Delay Detector Job
```
Expected local flow:
```
Simulator -> Kafka -> Flink -> Kafka
```
Repository Structure
```
delayed-order-sms-flink/
  README.md
  docker-compose.yml
  Makefile

  docs/
    adr/
      0001-processing-time-vs-event-time.md
      0002-sms-idempotency-strategy.md
    rfc/
      delayed-order-detection-rfc.md
    runbooks/
      local-runbook.md
      failure-test-runbook.md
      production-runbook.md

  schemas/
    order-events/
      order-created.schema.json
      order-delivered.schema.json
      order-cancelled.schema.json
      order-eta-updated.schema.json
    sms-commands/
      send-delay-sms-command.schema.json

  simulator/
    README.md
    scenarios/
      on-time-order.json
      delayed-order.json
      cancelled-order.json
      duplicate-events.json
      out-of-order-events.json
      eta-updated-order.json
    src/

  flink-job/
    README.md
    pom.xml
    src/
      main/
      test/

  e2e-tests/
    README.md
    scenarios/

  proposal/
    production-proposal.md
```
## Kafka Topics
The POC uses the following Kafka topics:
```
order-events
sms-commands
dead-letter-events
```
### order-events
Input topic for order lifecycle events.

### sms-commands
Output topic for SMS command events.

### dead-letter-events
Invalid, malformed, or unsupported events can be routed here.

### Processing Logic
The Flink job processes events keyed by orderId.

Simplified flow:
```
Kafka Source: order-events
        |
        v
Deserialize JSON
        |
        v
keyBy(orderId)
        |
        v
KeyedProcessFunction
        |
        v
Kafka Sink: sms-commands
```
For each order, the job keeps state such as:
```
orderId
customerId
storeId
currentStatus
expectedDeliveryTime
delaySmsEmitted
lastEventTime
registeredTimerTime
```
On ORDER_CREATED
Store order information in keyed state.
Register a processing-time timer for expectedDeliveryTime.
On ORDER_DELIVERED
Mark the order as delivered.
Do not emit SMS if it was delivered before the deadline.
On ORDER_CANCELLED
Mark the order as cancelled.
Do not emit delay SMS.
On Timer
When the timer fires:

Read the order state.
If the order is not delivered and not cancelled:
Emit SEND_DELAY_SMS.
Mark delaySmsEmitted = true.
## Simulator Scenarios
The simulator should support the following scenarios:

1. On-Time Order
Order is created and delivered before expected delivery time.

Expected result:
```
No SMS command should be emitted.
```
2. Delayed Order
Order is created but not delivered before expected delivery time.

Expected result:
```
One SEND_DELAY_SMS command should be emitted.
```
3. Cancelled Order
Order is created and cancelled before expected delivery time.

Expected result:
```
No SMS command should be emitted.
```
4. Duplicate Events
Same event is sent more than once.

Expected result:
```
No duplicate SMS command should be emitted.
```
5. Out-of-Order Events
Events arrive in a different order than their actual event time.

Expected result:
```
System should behave according to the defined business rules.
```
6. ETA Updated Order
Order expected delivery time is updated.

Expected result:
```
Timer should be updated based on the latest expectedDeliveryTime.
```
## Running Locally
Commands may be updated as the project evolves.

Start Local Infrastructure
```
docker compose up -d
```
Check Containers
```
docker compose ps
```
Open Flink UI
```
http://localhost:8081
```
Open Kafka UI
```
http://localhost:8080
```
Running the Simulator
Example commands:
```
make simulate-on-time-order
make simulate-delayed-order
make simulate-cancelled-order
make simulate-duplicates
make simulate-out-of-order
make simulate-eta-updated-order
```
Running the Flink Job
Example:
```
make build-flink-job
make submit-flink-job
```
Or manually:
```
cd flink-job
mvn clean package
```
Then submit the generated JAR to the local Flink cluster.

## Testing the Pipeline
Test: Delayed Order
Start local environment:
```
docker compose up -d
```
Submit Flink job.
Run delayed order scenario:
```
make simulate-delayed-order
```
Check sms-commands topic.
Expected output:
```
{
  "commandId": "ord-xxx:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS"
}
```
Test: On-Time Order
```
make simulate-on-time-order
```
Expected result:
```
No SEND_DELAY_SMS command should be emitted.
```
Test: Cancelled Order
```
make simulate-cancelled-order
```
Expected result:
```
No SEND_DELAY_SMS command should be emitted.
Failure Recovery Test
This project should verify that state and timers survive failure when checkpointing is enabled.
```
Basic scenario:
```
1. Start Kafka and Flink.
2. Submit Flink job.
3. Produce ORDER_CREATED with expectedDeliveryTime = now + 2 minutes.
4. Wait 30 seconds.
5. Kill TaskManager or cancel/restart the job.
6. Recover from checkpoint or savepoint.
7. Wait until expectedDeliveryTime passes.
8. Verify that only one SEND_DELAY_SMS command is emitted.
```
Details should be documented in:
```
docs/runbooks/failure-test-runbook.md
```
Checkpointing
The Flink job should enable checkpointing for fault tolerance.

The POC should validate:

State recovery
Timer recovery
Kafka offset recovery
No duplicate SMS commands after restart
## Idempotency
Duplicate SMS must be avoided at two levels:

1. Inside Flink
The Flink state contains:
```
delaySmsEmitted = true
```
After the SMS command is emitted once, the job should not emit it again for the same order.

2. Downstream Notification Service
The output command contains an idempotent key:
```
commandId = orderId + ":DELAY_SMS"
```
The SMS consumer should deduplicate based on this commandId.

## Production Considerations
The production proposal should cover:

Architecture
Kafka topic design
Event contracts
Schema evolution
Processing semantics
Checkpoint strategy
Savepoint and deployment strategy
Idempotency
Failure recovery
Monitoring
Alerting
Security
Access control
Replay strategy
Rollback strategy
Risks and mitigations
The final proposal should be located at:
```
proposal/production-proposal.md
```
## Important Production Note
Flink should not call the SMS provider directly.

Recommended production flow:
```
Flink -> Kafka sms-commands -> Notification Service -> SMS Provider
```
This keeps the Flink job focused on stream processing and avoids tight coupling with external providers, rate limits, retries, and SMS delivery concerns.

## Workshop Outcome
By the end of the workshop, this repository should contain:

A working local streaming environment.
An order event simulator.
A Flink job for delayed order detection.
Kafka topics for input and output.
Scenario-based tests.
Failure recovery experiments.
Technical design documents.
Production proposal.
## Contribution Guidelines
Every team member should contribute through Pull Requests.

Each PR should include:

What problem it solves.
What changes were made.
How it was tested.
Related issue number.
Screenshots or logs if useful.
Example branch names:
```
feature/docker-compose-local-env
feature/order-event-simulator
feature/flink-kafka-source
feature/delay-detector-state-timer
docs/production-proposal
fix/simulator-duplicate-events
```
## Definition of Done
The POC is considered complete when:

Local environment starts successfully.
Kafka and Flink are available locally.
Simulator can produce order events.
Flink job consumes order-events.
Flink job emits SEND_DELAY_SMS to sms-commands.
Delayed order scenario works.
On-time order scenario does not emit SMS.
Cancelled order scenario does not emit SMS.
Duplicate events do not produce duplicate SMS commands.
Checkpointing is enabled.
Failure recovery is tested.
README and runbooks are updated.
Production proposal is prepared.

## License
Internal use only.

## Maintainers
TBD
