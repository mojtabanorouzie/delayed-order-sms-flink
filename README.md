In this POC, the Notification Service and SMS Provider are not implemented.
The Flink job only produces SEND_DELAY_SMS commands into Kafka.

Main Use Case
When an order is created, it has an expectedDeliveryTime.

If the order is not delivered or cancelled before that time, the Flink job emits a delay SMS command.

Example:

text

Collapse
Save
Copy
1
2
3
4
5
ORDER_CREATED at 18:00
expectedDeliveryTime = 18:45

If no ORDER_DELIVERED or ORDER_CANCELLED event is received before 18:45,
emit SEND_DELAY_SMS command.
Order Events
The input stream contains order lifecycle events.

Supported event types:

text

Collapse
Save
Copy
1
2
3
4
5
6
ORDER_CREATED
ORDER_ACCEPTED
ORDER_PICKED_UP
ORDER_DELIVERED
ORDER_CANCELLED
ORDER_ETA_UPDATED
Example ORDER_CREATED event:

json

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
⌄
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
Example ORDER_DELIVERED event:

json

Collapse
Save
Copy
1
2
3
4
5
6
7
8
⌄
{
  "eventId": "evt-1002",
  "eventType": "ORDER_DELIVERED",
  "orderId": "ord-123",
  "eventTime": "2026-05-12T19:20:00Z",
  "occurredAt": "2026-05-12T19:20:05Z",
  "schemaVersion": 1
}
Output Command
When an order is detected as delayed, the Flink job emits a command to the sms-commands topic.

Example SEND_DELAY_SMS command:

json

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
⌄
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
The commandId must be idempotent:

text

Collapse
Save
Copy
1
commandId = orderId + ":DELAY_SMS"
This helps downstream services avoid sending duplicate SMS messages.

Time Semantics
For this POC, the delay detection trigger is based on Processing Time.

Reason:

The business question is:
"Has the real current time passed the expected delivery time?"
Therefore, the timer should fire based on wall-clock time.
However, input events still include eventTime for observability, debugging, ordering analysis, and future event-time processing needs.

Local Development Environment
The local environment should include:

Apache Kafka
Apache Flink JobManager
Apache Flink TaskManager
Kafka UI / Redpanda Console
Order Event Simulator
Flink Delay Detector Job
Expected local flow:

text

Collapse
Save
Copy
1
Simulator -> Kafka -> Flink -> Kafka
Repository Structure
text

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
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
Kafka Topics
The POC uses the following Kafka topics:

text

Collapse
Save
Copy
1
2
3
order-events
sms-commands
dead-letter-events
order-events
Input topic for order lifecycle events.

sms-commands
Output topic for SMS command events.

dead-letter-events
Invalid, malformed, or unsupported events can be routed here.

Processing Logic
The Flink job processes events keyed by orderId.

Simplified flow:

text

Collapse
Save
Copy
1
2
3
4
5
6
7
8
9
10
11
12
13
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
For each order, the job keeps state such as:

text

Collapse
Save
Copy
1
2
3
4
5
6
7
8
orderId
customerId
storeId
currentStatus
expectedDeliveryTime
delaySmsEmitted
lastEventTime
registeredTimerTime
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
Simulator Scenarios
The simulator should support the following scenarios:

1. On-Time Order
Order is created and delivered before expected delivery time.

Expected result:

text

Collapse
Save
Copy
1
No SMS command should be emitted.
2. Delayed Order
Order is created but not delivered before expected delivery time.

Expected result:

text

Collapse
Save
Copy
1
One SEND_DELAY_SMS command should be emitted.
3. Cancelled Order
Order is created and cancelled before expected delivery time.

Expected result:

text

Collapse
Save
Copy
1
No SMS command should be emitted.
4. Duplicate Events
Same event is sent more than once.

Expected result:

text

Collapse
Save
Copy
1
No duplicate SMS command should be emitted.
5. Out-of-Order Events
Events arrive in a different order than their actual event time.

Expected result:

text

Collapse
Save
Copy
1
System should behave according to the defined business rules.
6. ETA Updated Order
Order expected delivery time is updated.

Expected result:

text

Collapse
Save
Copy
1
Timer should be updated based on the latest expectedDeliveryTime.
Running Locally
Commands may be updated as the project evolves.

Start Local Infrastructure
bash

Collapse
Save
Copy
1
docker compose up -d
Check Containers
bash

Collapse
Save
Copy
1
docker compose ps
Open Flink UI
text

Collapse
Save
Copy
1
http://localhost:8081
Open Kafka UI
text

Collapse
Save
Copy
1
http://localhost:8080
Running the Simulator
Example commands:

bash

Collapse
Save
Copy
1
2
3
4
5
6
make simulate-on-time-order
make simulate-delayed-order
make simulate-cancelled-order
make simulate-duplicates
make simulate-out-of-order
make simulate-eta-updated-order
Running the Flink Job
Example:

bash

Collapse
Save
Copy
1
2
make build-flink-job
make submit-flink-job
Or manually:

bash

Collapse
Save
Copy
1
2
cd flink-job
mvn clean package
Then submit the generated JAR to the local Flink cluster.

Testing the Pipeline
Test: Delayed Order
Start local environment:
bash

Collapse
Save
Copy
1
docker compose up -d
Submit Flink job.
Run delayed order scenario:
bash

Collapse
Save
Copy
1
make simulate-delayed-order
Check sms-commands topic.
Expected output:

json

Collapse
Save
Copy
1
2
3
4
⌄
{
  "commandId": "ord-xxx:DELAY_SMS",
  "commandType": "SEND_DELAY_SMS"
}
Test: On-Time Order
bash

Collapse
Save
Copy
1
make simulate-on-time-order
Expected result:

text

Collapse
Save
Copy
1
No SEND_DELAY_SMS command should be emitted.
Test: Cancelled Order
bash

Collapse
Save
Copy
1
make simulate-cancelled-order
Expected result:

text

Collapse
Save
Copy
1
No SEND_DELAY_SMS command should be emitted.
Failure Recovery Test
This project should verify that state and timers survive failure when checkpointing is enabled.

Basic scenario:

text

Collapse
Save
Copy
1
2
3
4
5
6
7
8
1. Start Kafka and Flink.
2. Submit Flink job.
3. Produce ORDER_CREATED with expectedDeliveryTime = now + 2 minutes.
4. Wait 30 seconds.
5. Kill TaskManager or cancel/restart the job.
6. Recover from checkpoint or savepoint.
7. Wait until expectedDeliveryTime passes.
8. Verify that only one SEND_DELAY_SMS command is emitted.
Details should be documented in:

text

Collapse
Save
Copy
1
docs/runbooks/failure-test-runbook.md
Checkpointing
The Flink job should enable checkpointing for fault tolerance.

The POC should validate:

State recovery
Timer recovery
Kafka offset recovery
No duplicate SMS commands after restart
Idempotency
Duplicate SMS must be avoided at two levels:

1. Inside Flink
The Flink state contains:

text

Collapse
Save
Copy
1
delaySmsEmitted = true
After the SMS command is emitted once, the job should not emit it again for the same order.

2. Downstream Notification Service
The output command contains an idempotent key:

text

Collapse
Save
Copy
1
commandId = orderId + ":DELAY_SMS"
The SMS consumer should deduplicate based on this commandId.

Production Considerations
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

text

Collapse
Save
Copy
1
proposal/production-proposal.md
Important Production Note
Flink should not call the SMS provider directly.

Recommended production flow:

text

Collapse
Save
Copy
1
Flink -> Kafka sms-commands -> Notification Service -> SMS Provider
This keeps the Flink job focused on stream processing and avoids tight coupling with external providers, rate limits, retries, and SMS delivery concerns.

Workshop Outcome
By the end of the workshop, this repository should contain:

A working local streaming environment.
An order event simulator.
A Flink job for delayed order detection.
Kafka topics for input and output.
Scenario-based tests.
Failure recovery experiments.
Technical design documents.
Production proposal.
Contribution Guidelines
Every team member should contribute through Pull Requests.

Each PR should include:

What problem it solves.
What changes were made.
How it was tested.
Related issue number.
Screenshots or logs if useful.
Example branch names:

text

Collapse
Save
Copy
1
2
3
4
5
6
feature/docker-compose-local-env
feature/order-event-simulator
feature/flink-kafka-source
feature/delay-detector-state-timer
docs/production-proposal
fix/simulator-duplicate-events
Definition of Done
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
License
Internal use only.

Maintainers
TBD

Status
text

Collapse
Save
Copy
1
POC / Workshop Project

Collapse

Run
Save
Copy
1
2
3
4
5
6
7
8
9

---

# GitHub/GitLab Short Description

برای فیلد description خود repo این را بگذار:

```text
Flink-based POC for real-time delayed order detection and idempotent SMS command generation from Kafka order events.
