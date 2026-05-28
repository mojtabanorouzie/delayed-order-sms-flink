# Production Proposal: Delayed Order SMS Detection System

**Date:** 2025-03-01
**Author:** Delayed Order SMS Team
**Status:** Draft

---

## Executive Summary

This proposal outlines the plan to move the Delayed Order SMS Detection system from a proof-of-concept (POC) to a production-grade deployment. The system monitors order lifecycle events in real-time and notifies customers via SMS when their order exceeds the expected delivery time, improving customer satisfaction and reducing support inquiries.

---

## Business Case

### Problem Statement

Currently, customers receive no proactive notification when their orders are delayed. This leads to:

- **Customer frustration:** Customers check order status manually, leading to dissatisfaction.
- **Support overhead:** Delayed orders generate a significant volume of support tickets.
- **Brand reputation:** Lack of communication during delays damages brand trust.

### Proposed Solution

A real-time streaming pipeline that:

1. Monitors all order state changes from Kafka (compacted `Orders` topic).
2. Detects when an order's `expectedDeliveryTime` passes without delivery.
3. Emits idempotent SMS commands to a downstream SMS service via Kafka (`sms-commands` topic).
4. Provides dead letter queuing for invalid events.
5. Exposes operational metrics for monitoring and alerting.

### Expected Benefits

| Benefit | Metric | Target |
|---------|--------|--------|
| Reduced support tickets | # tickets related to "order delay" | -30% within 3 months |
| Improved CSAT | Post-delivery survey score | +5 points |
| Faster delay notification | Time from ETA breach to SMS | < 2 minutes |
| Operational visibility | Dashboards + alerts | Real-time metrics within week 1 |

---

## Architecture

### High-Level Architecture

The system follows a stream-processing pattern:

```
Order Service → Kafka (Orders) → Flink (delayed detection) → Kafka (sms-commands) → SMS Service
                                                             → Kafka (dead-letter-events) → Monitoring
```

### Key Architecture Decisions

| Decision | Rationale | Reference |
|----------|-----------|-----------|
| Processing-time timers | Simpler, wall-clock aligned, ok for POC→prod | ADR-0001 |
| Idempotency via `commandId` | Prevents duplicate SMS across restarts | ADR-0002 |
| RocksDB state backend | Persistent state, incremental checkpoints | Task 1 fix |
| State TTL (7 days) | Prevents unbounded state growth | Task 5 |
| Dead Letter Queue | Captures invalid events for debugging | Task 3 |
| Custom Flink metrics | Operational visibility | Task 4 |
| Kubernetes + Flink Operator | Production orchestration, savepoint upgrades | Production runbook |

### Data Flow Diagram

See `docs/rfc/delayed-order-detection-rfc.md` Section 2 for detailed Mermaid and ASCII diagrams.

---

## Infrastructure Requirements

### Production Environment

| Component | Specification | Notes |
|-----------|--------------|-------|
| **Kafka Cluster** | 3 brokers, 12 partitions for key topics | Existing Kafka infrastructure |
| **Flink Cluster** | 2 JobManagers (HA), 4 TaskManagers | Auto-scaling via K8s HPA |
| **State Storage** | S3 (or HDFS), 100GB initial allocation | Incremental checkpoints minimize storage |
| **Kubernetes** | Cluster with 8+ nodes (4 CPU/16GB each) | Dedicated node group for Flink TMs |
| **Monitoring** | Prometheus + Grafana (existing) | Add Flink-specific dashboards |
| **Alerting** | PagerDuty (critical), Slack (warnings) | Integrated with existing on-call rotation |

### Resource Estimates

| Resource | POC | Production | Notes |
|----------|-----|-----------|-------|
| Flink JobManagers | 1 × 1 CPU / 2GB | 2 × 2 CPU / 4GB | HA pair |
| Flink TaskManagers | 1 × 2 CPU / 4GB | 4 × 4 CPU / 8GB | 4 slots each = 16 total |
| Kafka throughput | N/A (test) | 1,000 msg/s avg, 5,000 peak | Based on order volume projections |
| State size (steady) | N/A | ~5GB | ~500K active orders × ~1KB each |
| Checkpoint size | N/A | ~5GB per checkpoint | Incremental diffs ~100MB |

---

## Cost Estimate

### Infrastructure (Monthly)

| Item | Unit Cost | Quantity | Monthly Cost |
|------|----------|----------|--------------|
| K8s nodes (TM) | $200/node | 4 | $800 |
| K8s nodes (JM HA) | $150/node | 2 | $300 |
| S3 storage (checkpoints) | $0.023/GB | 200GB | $4.60 |
| S3 API requests | $0.005/1k PUT | ~500k | $2.50 |
| Kafka (existing, allocated) | $0 (internal) | — | $0 |
| Prometheus/Grafana (existing) | $0 (internal) | — | $0 |

**Total estimated monthly cost: ~$1,107**

### SMS Costs (Variable)

SMS cost is incurred by the downstream SMS service, not this Flink job. However, for planning:

- Estimated delayed orders: 50,000/month
- SMS cost: ~$0.05/SMS (varies by country)
- **Total SMS cost: ~$2,500/month**

---

## Migration Plan

### Phase 1: Staging Deployment (Week 1-2)

- [ ] Deploy Flink Operator to staging K8s cluster
- [ ] Apply `FlinkDeployment` CR with staging Kafka
- [ ] Run E2E test suite (all 6 scenarios)
- [ ] Validate metrics pipeline (Prometheus scrape config)
- [ ] Validate alerting rules (test alerts fire)
- [ ] Run 24-hour soak test with simulated load

### Phase 2: Shadow Mode (Week 3)

- [ ] Deploy to production K8s cluster
- [ ] Configure to read from production `Orders` topic
- [ ] Write to `sms-commands-shadow` topic (not consumed by SMS service)
- [ ] Monitor for 1 week:
  - Verify throughput matches projections
  - Verify no parse errors on production data
  - Verify state size and checkpoint behavior
  - Verify TTL cleanup works as expected

### Phase 3: Production Cutover (Week 4)

- [ ] Switch sink to production `sms-commands` topic
- [ ] SMS service begins consuming from `sms-commands`
- [ ] Enable SMS delivery (initially 10% of orders, ramping to 100% over 3 days)
- [ ] Monitor CSAT and support ticket volume
- [ ] Full rollback plan tested and documented

### Phase 4: Optimization (Week 5-8)

- [ ] Tune parallelism based on observed load
- [ ] Optimize checkpoint interval and timeout
- [ ] Tune RocksDB memory settings
- [ ] Set up S3 lifecycle policies for checkpoint cleanup
- [ ] Create operational runbooks (incident response)

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Duplicate SMS sent to customers | Low | High | Idempotent `commandId`; downstream dedup; `delaySmsEmitted` boolean |
| Kafka outage causes processing gap | Medium | Medium | Flink auto-restarts from checkpoint; catch-up on recovery |
| Parse errors from malformed upstream data | Medium | Low | DLQ captures; alert on `parse_errors`; fix upstream |
| State grows unbounded (no TTL) | Low | Medium | 7-day TTL configured; alert on state size growth |
| Checkpoint failure / state corruption | Low | High | Incremental checkpoints; S3 durability; restore from prior checkpoint |
| Flink Operator upgrade breaks deployment | Low | Medium | Test on staging first; savepoint before upgrade |
| SMS service unavailable | Medium | Low | `sms-commands` topic retains messages; SMS service catches up |
| Cost overrun (SMS) | Low | Low | `sms_commands_emitted` metric tracked; alert on spike |

### Rollback Plan

If critical issues are discovered post-cutover:

1. Suspend Flink deployment (`state: suspended`) — automatic savepoint created.
2. Disable SMS service consumption from `sms-commands` topic.
3. Revert Flink job to shadow mode (write to `sms-commands-shadow`).
4. Investigate and fix the issue.
5. Re-deploy and resume from savepoint.

**Maximum rollback time: < 10 minutes.**

---

## Dependencies

| Dependency | Owner | Status |
|-----------|-------|--------|
| Kafka cluster (production) | Platform/Infra team | Available |
| Kubernetes cluster | Platform/Infra team | Available |
| S3 bucket for checkpoints | Infra team | Needs creation |
| Flink Operator installation | DevOps team | Needs installation |
| SMS service (dedup support) | Notifications team | Under development |
| Prometheus/Grafana | Observability team | Available (needs scrape config) |
| Container registry | DevOps team | Available |

---

## Success Metrics

### Technical Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Processing latency (P99) | < 10 seconds | Flink metrics |
| Checkpoint success rate | > 99.9% | Flink metrics |
| Parse error rate | < 0.01% | `parse_errors` / `numRecordsIn` |
| Job uptime | > 99.9% | Flink metrics |
| State TTL cleanup | Orders cleaned up within 7 days + 1 hour | State size monitoring |

### Business Metrics (Post-Launch)

| Metric | Baseline | Target (Month 3) |
|--------|----------|-------------------|
| Support tickets (delay-related) | 2,000/month | < 1,400/month |
| CSAT (delivery experience) | 3.8/5 | > 4.2/5 |
| Time-to-notify (ETA breach → SMS) | N/A (no system) | < 2 minutes (P99) |
| SMS delivery success rate | N/A | > 95% |

---

## Appendix

### Reference Documents

- [ADR-0001: Processing Time vs Event Time](../docs/adr/0001-processing-time-vs-event-time.md)
- [ADR-0002: SMS Idempotency Strategy](../docs/adr/0002-sms-idempotency-strategy.md)
- [RFC: Delayed Order Detection](../docs/rfc/delayed-order-detection-rfc.md)
- [Production Runbook](../docs/runbooks/production-runbook.md)
- [Local Development Runbook](../docs/runbooks/local-runbook.md)
- [Failure Test Runbook](../docs/runbooks/failure-test-runbook.md)

### Glossary

| Term | Definition |
|------|------------|
| ETA | Expected Delivery Time |
| SMS | Short Message Service (text message) |
| DLQ | Dead Letter Queue |
| TTL | Time To Live |
| TM | TaskManager (Flink worker node) |
| JM | JobManager (Flink coordinator node) |
| CSAT | Customer Satisfaction Score |
| HA | High Availability |