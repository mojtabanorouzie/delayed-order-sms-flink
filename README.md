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
