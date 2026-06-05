# Production Proposal: Delayed Order SMS Detection System

**Date:** 2026-06-06
**Author:** Senior Infrastructure Manager
**Reviewers Required:** Platform Lead, Engineering Manager, Notifications Team, Security
**Status:** DRAFT — Pending sign-off before any production deployment

---

## Executive Summary

This proposal approves the path from POC to production for the Delayed Order SMS Detection system at **1,000 orders/minute** sustained throughput (5,000/minute peak). The system must reliably detect ETA breaches and emit idempotent SMS commands with < 2-minute P99 latency and 99.9% job uptime.

**Target go-live: 8 weeks from sign-off.**

The original draft proposal contained incorrect capacity math, understated SMS costs by two orders of magnitude, omitted security requirements, and incorrectly described Flink auto-scaling. This rewrite corrects those gaps and establishes concrete go/no-go gates for each deployment phase.

**One hard blocker before Phase 1 begins: the SMS cost at this scale is ~$43,000–$108,000/month (not $2,500). Finance sign-off on that budget is required before any production work starts.**

---

## Scope and Non-Goals

**In scope:**

- Real-time ETA breach detection via Flink processing-time timers
- Idempotent SMS command emission to `sms-commands` Kafka topic
- Dead-letter queue for malformed/invalid events
- Production K8s deployment via Flink Operator
- Full observability (metrics, dashboards, alerts, on-call runbooks)

**Out of scope:**

- SMS delivery (owned by Notifications team)
- Order lifecycle management (owned by Order Service team)
- Customer communication preferences / opt-out
- Analytics or historical reporting

---

## Capacity Analysis at 1,000 Orders/Minute

Everything in this proposal is derived from these numbers. If order volume assumptions change, revisit this section first.

### Throughput Model

```text
Input rate:         1,000 orders/min sustained  (16.7 orders/sec)
Peak rate:          3,000 orders/min            (50 orders/sec)   ← design target
Event multiplier:   ~4 state transitions/order  (CREATED → ACCEPTED → PICKED_UP → DELIVERED)

Kafka ingestion:    4,000 events/min sustained  (67 events/sec)
Kafka peak:        12,000 events/min peak       (200 events/sec)

Daily orders:       1,440,000 orders/day (at sustained 1k/min)
Monthly orders:    ~43,200,000 orders/month
```

### Active State Sizing (RocksDB)

```text
Avg order-in-flight time:   ~60 minutes
Active orders at 1k/min:    60 min × 1,000/min = 60,000 active orders
State per order:            ~1 KB (OrderDelayState POJO + RocksDB overhead)
Active state footprint:     ~60 MB (trivial for RocksDB)

TTL safety net (7 days):    Orders with no terminal event accumulate
  Assumption: 0.1% of orders stuck = 1,440/day × 7 days = ~10,080 stuck orders
  Additional state:          ~10 MB

Total steady-state RocksDB: ~70 MB — well within a single TM's off-heap budget
```

### Checkpoint Budget

```text
Full checkpoint (initial):  ~70 MB
Incremental diffs:          ~500K events/10s × avg delta → ~2–5 MB per checkpoint
S3 storage (30 checkpoints retained):  30 × 5 MB = 150 MB ≈ negligible cost
```

> **Conclusion:** State and checkpoint sizes at this throughput are much smaller than the original proposal assumed. The original 5 GB and 500 K active-order estimates were for a 10× larger system.

### Kafka Partition Sizing

```text
Max Flink parallelism = Kafka partition count for the Orders topic.
Target parallelism:    8 (provides 4× headroom over current 67 events/sec)
Max future parallelism: 24 (headroom to ~10,000 orders/minute before repartitioning)

Recommendation: Create Orders topic with 24 partitions.
  Current POC has 3 partitions — this MUST be changed before production.
  sms-commands: 24 partitions (matches upstream for downstream scaling)
  dead-letter-events: 6 partitions (low volume)
```

### Flink Parallelism Decision

```text
Initial parallelism:    4  (handles up to ~800 events/sec with headroom)
Scale-up trigger:       Kafka consumer lag > 10,000 messages for > 5 minutes
Scale-up action:        Take savepoint → redeploy at parallelism 8
Max planned parallelism: 12 (before requiring Kafka repartitioning)

Note: Flink parallelism is NOT changed via K8s HPA.
HPA does not work with stateful Flink jobs.
Scaling requires a savepoint + redeployment — plan accordingly.
```

---

## Architecture

### Data Flow

```text
Order Service
    │ (compacted, keyed by orderId)
    ▼
Kafka: Orders topic (24 partitions, RF=3, min.ISR=2)
    │
    ▼ KafkaSource (committed offsets, not .latest())
Flink: OrderStateDeserializationFunction (FlatMap)
    ├─ parse error → side output
    │                    │
    │                    ▼
    │           Kafka: dead-letter-events (6 partitions)
    │
    ▼ keyBy(orderId)
Flink: DelayedOrderProcessFunction
    (KeyedProcessFunction, processing-time timers, RocksDB state)
    │
    ▼ SmsCommand (keyed by commandId)
Kafka: sms-commands (24 partitions, RF=3)
    │
    ▼
SMS Service (deduplicates by commandId)
```

### Key Architecture Decisions

| Decision | Rationale | Risk |
|----------|-----------|------|
| Processing-time timers | Wall-clock aligned; no watermark complexity | Orders crossing ETA during a job outage may miss notification if they complete (DELIVERED/CANCELLED) before Flink recovers. Acceptable for SMS use case. |
| `commandId = orderId:DELAY_SMS` | Deterministic idempotency key; survives restarts | SMS service **must** deduplicate — this is a hard contract dependency. |
| RocksDB state backend | ~70 MB state fits easily; incremental checkpoints cheap | RocksDB compaction adds ~5% CPU overhead. Acceptable at this throughput. |
| 7-day TTL | Safety net for stuck orders | Orders stuck for exactly 7 days emit no SMS. Business accepted. |
| EXACTLY_ONCE checkpointing | Prevents duplicate or lost SMS commands across restarts | Doubles Kafka producer latency (two-phase commit). At 67 events/sec this is negligible. |

### Processing-Time Timer Gap (Known Limitation)

If the Flink job is offline for > 2 minutes, orders that breach their ETA during the outage will not receive SMS until Flink recovers — and only if they are still in an active state when recovery completes. This is acceptable for SMS (vs. financial transactions) but must be documented in the on-call runbook and communicated to Product.

---

## Infrastructure Specification

### Kafka (Production)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Brokers | 3 | Minimum for RF=3 durability |
| Replication factor | 3 (all topics) | Data durability; survive single broker loss |
| `min.insync.replicas` | 2 | Requires 2 in-sync replicas before ACK; producer uses `acks=all` |
| Orders partitions | 24 | Max Flink parallelism; headroom to 10k orders/min |
| `sms-commands` partitions | 24 | Matches upstream; SMS service can scale independently |
| `dead-letter-events` partitions | 6 | Low volume |
| Orders retention | compacted + `min.compaction.lag.ms=3600000` | Preserve at least 1 hour of history for replay |
| `sms-commands` retention | 7 days (delete) | Buffer for SMS service recovery |
| Orders `min.insync.replicas` | 2 | Writes fail rather than silently losing durability |
| Kafka ACLs | Read: Flink consumer group; Write: Order Service only | Principle of least privilege |
| TLS | Required (in-flight encryption) | Security requirement |

> **Action required:** Confirm with Kafka platform team that the existing cluster supports RF=3 with `min.insync.replicas=2` for new topics. The POC uses single-broker KRaft — this is **not** a valid production topology.

### Flink (Production — Flink Operator on K8s)

**JobManagers (HA pair):**

| Parameter | Value |
|-----------|-------|
| Replicas | 2 (HA via ZooKeeper or K8s HA store) |
| CPU request/limit | 1 / 2 |
| Memory request/limit | 2 Gi / 2 Gi (Guaranteed QoS) |

**TaskManagers:**

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Replicas | 2 (initial) | Parallelism 4; 2 slots per TM |
| CPU request/limit | 2 / 2 (Guaranteed QoS) | Prevents CPU throttling during checkpoint |
| Memory request/limit | 4 Gi / 4 Gi | Guaranteed QoS — critical for RocksDB off-heap |
| Task slots per TM | 2 | Simpler memory isolation; easier to reason about |
| JVM heap (`-Xmx`) | 1.5 Gi | Flink managed memory and network buffers |
| RocksDB block cache | 512 Mi | Shared across slots; fits in off-heap budget |
| RocksDB write buffer | 128 Mi | Per TM |
| Managed memory fraction | 0.4 | ~1.6 Gi per TM for RocksDB |

**Flink Job Configuration Overrides:**

```yaml
parallelism.default: 4
state.backend: rocksdb
state.backend.rocksdb.memory.managed: true
state.backend.incremental: true
execution.checkpointing.interval: 30000          # 30s — reduced from 10s (see rationale)
execution.checkpointing.timeout: 120000          # 2 minutes
execution.checkpointing.min-pause: 20000         # Minimum 20s between checkpoints
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.max-concurrent-checkpoints: 1
state.checkpoints.num-retained: 5               # Keep 5 checkpoints on S3
restart-strategy: exponential-delay             # NOT fixed-delay
restart-strategy.exponential-delay.initial-backoff: 5s
restart-strategy.exponential-delay.max-backoff: 5min
restart-strategy.exponential-delay.max-attempts: 2147483647  # Effectively unlimited
```

> **Checkpoint interval rationale:** 10s (POC default) is too aggressive for production. At 10s, a checkpoint must complete in <10s or it delays the next one, creating a backlog. At 67 events/sec, 30s gives 2,000 events per checkpoint window — comfortable margin. 30s also means max data re-processed after restart = 30s of events (~2,000 events, ~2,000 timer re-evaluations).

> **Restart strategy rationale:** Fixed-delay with 3 attempts means a transient Kafka hiccup causes the job to permanently fail, requiring manual intervention at 3 AM. Exponential backoff with unlimited retries keeps the job self-healing. The `delaySmsEmitted` boolean and `commandId` idempotency handle any duplicate processing on restart.

### Kubernetes Resources

**Node configuration:**

| Node Group | Purpose | Instance | Count |
|-----------|---------|----------|-------|
| `flink-tm` | TaskManagers | 4 vCPU / 8 GB | 2 (scales to 6) |
| `flink-jm` | JobManagers + Operator | 2 vCPU / 4 GB | 2 |

**Required K8s objects:**

```yaml
# PodDisruptionBudget for TaskManagers — prevents simultaneous eviction
spec:
  minAvailable: 1    # At least 1 TM must be running during node drain

# PodAntiAffinity for TaskManagers
requiredDuringScheduling:
  topologyKey: kubernetes.io/hostname    # TMs on different nodes

# PodAntiAffinity for JobManagers
requiredDuringScheduling:
  topologyKey: kubernetes.io/hostname    # JMs on different nodes

# ResourceQuota for flink namespace
requests.cpu: 10
requests.memory: 24Gi
limits.cpu: 12
limits.memory: 24Gi
```

**Node affinity:** `flink-tm` nodes must be in at least 2 different availability zones. Single-AZ Flink with multi-AZ Kafka creates a network-partition failure mode where Flink loses quorum and stops processing.

### S3 Checkpoint Storage

| Parameter | Value |
|-----------|-------|
| Bucket | `prod-flink-checkpoints-delayed-order-sms` |
| Region | Same as K8s cluster |
| Encryption | SSE-S3 (minimum); SSE-KMS preferred |
| Versioning | Disabled (Flink manages checkpoint lifecycle) |
| Lifecycle rule | Delete objects older than 30 days |
| Retained checkpoints | 5 (configured in Flink) |
| Estimated storage | <1 GB steady-state (70 MB × 5 incremental checkpoints) |
| Monthly cost | < $1 |

### Security Requirements (Currently Missing)

These are blocking requirements, not optional hardening:

| Control | Requirement | Owner |
|---------|------------|-------|
| Kafka mTLS | Flink ↔ Kafka must use TLS + client certificates | Platform team |
| Kafka ACLs | Consumer group `delayed-order-sms-flink` reads `Orders` only | Platform team |
| Flink web UI | Basic auth or OAuth2 proxy; not open to the internet | DevOps |
| K8s Network Policy | Flink pods may only reach Kafka brokers and S3 endpoint | Security team |
| Secrets management | Kafka credentials via K8s Secrets (not env vars in ConfigMap) | DevOps |
| Container image | No `latest` tag in production; digest-pinned images only | DevOps |
| RBAC | `FlinkDeployment` CR manageable only by CI/CD service account | DevOps |

---

## Known Implementation Gaps (Must Fix Before Phase 1)

These are bugs or missing features in the current codebase that will cause production incidents:

### P0 — Blockers

**1. Wrong Kafka offset initializer**

- Current: `OffsetsInitializer.latest()` — on restart, Flink reads from the end of the topic and skips events produced during downtime.
- Fix: Remove `.setStartingOffsets(OffsetsInitializer.latest())` in `DelayedOrderSmsJob.java`. Flink will use committed offsets by default, which is correct.

**2. Restart strategy fails permanently after 3 attempts**

- Current: Fixed-delay, 3 attempts. After 3 transient failures, the job dies permanently.
- Fix: Change to exponential-delay restart with unlimited retries (see Flink config above).

**3. Kafka topics have 3 partitions — caps parallelism at 3**

- Current: Orders, sms-commands, dead-letter-events all have 3 partitions.
- Fix: Recreate topics with 24 / 24 / 6 partitions before shadow mode. Topic recreation requires Kafka platform team coordination. **This is a one-way operation.**

### P1 — Required Before Shadow Mode

**4. No Kafka replication or `min.insync.replicas` config**

- Current: POC uses single broker with effective RF=1. All writes succeed even with 0 in-sync replicas.
- Fix: Production Kafka cluster with RF=3, `min.insync.replicas=2`, producer `acks=all`.

**5. Checkpoint interval is 10s (too aggressive)**

- Fix: Set `checkpoint.interval.ms=30000` (see configuration above).

**6. No S3 checkpoint retention policy**

- Fix: Set `state.checkpoints.num-retained=5` and configure S3 lifecycle rule (30-day delete).

**7. Flink web UI is unauthenticated**

- Fix: Deploy an OAuth2 proxy (e.g., `oauth2-proxy`) in front of port 8081 before production.

### P2 — Required Before Full Cutover

**8. No Kafka consumer lag alert**

- Consumer lag is the most important operational signal. If lag grows, Flink is falling behind.
- Fix: Add Prometheus alert: `kafka_consumer_lag_sum > 10000 for 5m` → PagerDuty.

**9. No Schema Registry / schema evolution strategy**

- Current: JSON is parsed with `@JsonIgnoreProperties(ignoreUnknown=true)` — tolerant, but no forward-compatibility guarantees.
- Fix: Define schema evolution policy: additive-only changes are safe; field removals require a 2-deploy migration. Document this contract with the Order Service team.

---

## Cost Analysis at 1,000 Orders/Minute

### Infrastructure (Monthly)

| Item | Unit Cost | Quantity | Monthly |
|------|----------|----------|---------|
| K8s nodes (TM) — 4 vCPU / 8 GB | $200/node | 2 | $400 |
| K8s nodes (JM) — 2 vCPU / 4 GB | $150/node | 2 | $300 |
| S3 storage (checkpoints) | $0.023/GB | 1 GB | $0.02 |
| S3 API (negligible) | — | — | < $1 |
| Kafka (existing, allocated) | $0 | — | $0 |
| Prometheus/Grafana (existing) | $0 | — | $0 |
| **Infrastructure total** | | | **~$701/month** |

### SMS Costs (Variable — Finance Approval Required)

The original proposal estimated $2,500/month based on 50,000 orders/month. **At 1,000 orders/minute this is wrong by a factor of 860×.**

```text
Orders/month:       1,000/min × 60 min × 24 hr × 30 days = 43,200,000/month
Delayed order rate: 3–7% (industry average for food delivery)
SMS volume:         1,296,000 – 3,024,000 SMS/month
SMS unit cost:      $0.03–$0.05 (varies by carrier and country mix)

Low estimate:       1,296,000 × $0.03 = $38,880/month
High estimate:      3,024,000 × $0.05 = $151,200/month
Midpoint estimate:  ~$75,000–$95,000/month
```

**This requires Finance and Product sign-off before production deployment. This is a hard gate.**

> If the delayed order rate or SMS cost is unacceptable, the business must decide whether to limit SMS to specific order segments (high-value orders, specific regions) before go-live. This is a product decision, not an engineering decision.

### Total Monthly Cost

| Component | Monthly |
|-----------|---------|
| Infrastructure | ~$701 |
| SMS (midpoint estimate) | ~$85,000 |
| **Total** | **~$86,000/month** |

---

## Deployment Plan

### Phase 0: Pre-Production Readiness (Week 0 — Before Any Deployment)

**Gate: All P0 and P1 gaps resolved. Finance approval on SMS cost. Security requirements signed off.**

- [ ] Fix `OffsetsInitializer.latest()` → committed offsets
- [ ] Change restart strategy to exponential-delay unlimited
- [ ] Update checkpoint interval to 30s
- [ ] Kafka platform team creates production topics (24/24/6 partitions, RF=3)
- [ ] S3 bucket created with encryption and lifecycle policy
- [ ] Flink Operator installed on staging K8s cluster
- [ ] Container registry accessible from K8s cluster
- [ ] Finance sign-off on SMS budget
- [ ] Security review of network policies and mTLS plan

### Phase 1: Staging Deployment (Week 1–2)

**Goal:** Validate the entire pipeline under simulated production load.

- [ ] Deploy Flink Operator to staging K8s cluster
- [ ] Apply `FlinkDeployment` CR with staging Kafka (RF=3 staging cluster or mirrored config)
- [ ] Run full E2E test suite (all 6 scenarios)
- [ ] Run 72-hour soak test at 1,000 events/minute synthetic load (not 24-hour — 24 hours is insufficient for observing RocksDB compaction behavior and checkpoint drift)
- [ ] Validate all Prometheus metrics scrape correctly
- [ ] Validate all Grafana dashboards render correctly
- [ ] Trigger each alert in staging and confirm PagerDuty/Slack routing
- [ ] Test savepoint → restore cycle (must complete in < 5 minutes)
- [ ] Test TM node drain (K8s eviction) and confirm job recovers from checkpoint
- [ ] Validate DLQ: inject malformed events, confirm dead-letter-events topic receives them

**Go/No-Go Gate for Phase 2:**

- Zero P0/P1 bugs observed in 72-hour soak
- Checkpoint success rate = 100% over soak
- Consumer lag < 1,000 at end of soak (Flink keeping up)
- All 6 E2E scenarios pass
- Savepoint/restore successfully tested

### Phase 2: Shadow Mode — Production Read (Week 3)

**Goal:** Validate behavior against real production order traffic without sending any SMS.

- [ ] Deploy to **production** K8s namespace
- [ ] Configure source: production `Orders` topic (real traffic)
- [ ] Configure sink: `sms-commands-shadow` topic (**not** consumed by SMS service)
- [ ] Configure dead-letter sink: `dead-letter-events-shadow`
- [ ] Run for 7 days minimum (captures weekly traffic patterns)
- [ ] Monitor:
  - Consumer lag (must stay < 5,000 during peak)
  - Parse error rate (must be < 0.1% — signals upstream schema issues)
  - State size trend (must be stable, not growing unboundedly)
  - Checkpoint duration (must be < 10s for 30s interval)
  - `delayed_orders_detected` rate (used to estimate actual SMS volume)

**Go/No-Go Gate for Phase 3:**

- Consumer lag < 5,000 at 3k orders/min peak
- Parse error rate < 0.1%
- Checkpoint success rate > 99.9% over 7 days
- `sms_commands_emitted` rate implies SMS cost within approved budget
- Zero job restarts that weren't self-healed within 5 minutes
- No unbounded state growth

### Phase 3: Canary Cutover (Week 4)

**Goal:** Begin sending real SMS notifications with controlled blast radius.

- [ ] SMS service team confirms dedup by `commandId` is implemented and tested
- [ ] Switch Flink sink from `sms-commands-shadow` to `sms-commands` (requires savepoint + redeploy)
- [ ] SMS service begins consuming from `sms-commands` at **10% of orders** (SMS service-side sampling)
- [ ] Monitor CSAT signal and support ticket volume for 48 hours
- [ ] Ramp to 50% at Day 3, 100% at Day 5 if metrics are healthy
- [ ] Flink job does **not** change — ramp is controlled at SMS service layer

**Rollback trigger:** Any of the following → immediately execute rollback:

- Duplicate SMS complaints from customers
- SMS delivery failure rate > 5%
- Flink job failing to self-heal within 10 minutes

### Phase 4: Steady State + Optimization (Week 5–8)

- [ ] Tune `state.ttl.days` based on observed stuck-order volume (may reduce from 7 days)
- [ ] Set S3 checkpoint lifecycle policy (delete after 30 days)
- [ ] Review RocksDB compaction metrics and tune if needed
- [ ] Establish parallelism scale-up runbook: at what consumer lag do we scale from 4 → 8?
- [ ] Review and finalize on-call runbook with Platform team
- [ ] Document schema evolution policy with Order Service team
- [ ] Conduct first game-day exercise: simulate TM node failure + recovery

---

## Rollback Plan

**Maximum time to safe state: < 5 minutes.**

1. `kubectl patch flinkdeployment delayed-order-sms --type=merge -p '{"spec":{"job":{"state":"suspended"}}}'`
   — Flink Operator triggers automatic savepoint. Job stops. No data is lost.
2. SMS service team disables consumption from `sms-commands` (or topic consumer group is paused).
3. Flink is reconfigured to write to `sms-commands-shadow` and redeployed from the savepoint.
4. Root cause is investigated using Flink logs, Grafana dashboards, and DLQ events.
5. Fix is validated in staging before re-cutover.

> The savepoint is the critical safety mechanism. Validate it works in Phase 1 before Phase 3 begins. Never skip this test.

---

## Observability

### SLOs

| SLO | Target | Measurement |
|-----|--------|-------------|
| Job uptime | > 99.9% (43 min/month downtime budget) | Flink `numRestarts` + uptime metric |
| Delay-to-notification latency (P99) | < 2 minutes | `expectedDeliveryTime` → `sms-commands` produced timestamp |
| Checkpoint success rate | > 99.9% | Flink `numberOfCompletedCheckpoints` / attempts |
| Parse error rate | < 0.01% | `parse_errors` / `numRecordsIn` |
| Kafka consumer lag | < 10,000 messages | Kafka consumer group lag metric |

### Dashboard Panels (Grafana)

**Row 1 — Health at a Glance:**
- Job status (UP/DOWN/RESTARTING)
- Consumer lag (time-series, alert threshold line at 10k)
- Checkpoint success rate (last 24h)
- SMS commands emitted (rate, last 1h)

**Row 2 — Throughput:**
- Events in/out per second (Flink `numRecordsIn`, `numRecordsOut`)
- `delayed_orders_detected` rate
- `stale_updates_ignored` rate (spikes indicate upstream event ordering issues)
- `invalid_messages` count

**Row 3 — State and Checkpoints:**
- RocksDB size (bytes)
- Last checkpoint duration (seconds)
- Last checkpoint size (bytes)
- Number of retained checkpoints

**Row 4 — JVM / Resource:**
- TM CPU usage (alert at > 80% sustained)
- TM heap usage
- RocksDB block cache hit ratio (alert if < 80%)
- Kafka producer errors

### Alerts

| Alert | Threshold | Severity | Action |
|-------|-----------|----------|--------|
| Consumer lag high | > 10,000 for 5m | WARNING | Investigate back-pressure; prepare to scale |
| Consumer lag critical | > 50,000 for 5m | CRITICAL (PagerDuty) | Scale parallelism or find blocking operation |
| Job restarting | `numRestarts` increasing for 5m | WARNING | Check restart logs |
| Job down | No metrics for 3m | CRITICAL (PagerDuty) | Check K8s pod status, Flink Operator logs |
| Checkpoint failed | `numberOfFailedCheckpoints` > 0 in 10m | WARNING | Check S3 connectivity, TM memory |
| Checkpoint duration | > 20s | WARNING | Investigate RocksDB compaction, GC |
| Parse error spike | > 0.1% for 5m | WARNING | Check `dead-letter-events` topic; notify upstream |
| SMS spike | `sms_commands_emitted` > 2× baseline for 10m | WARNING | Check for upstream data issue or mass delay event |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation | Owner |
|------|-----------|--------|------------|-------|
| SMS duplicate sent to customer | Low | High | `commandId` dedup at Flink + SMS service; `delaySmsEmitted` boolean prevents re-emission | Notifications team must confirm dedup |
| Job offline during ETA breach window | Medium | Medium | Processing-time timers fire on recovery for still-active orders; outage window is bounded by checkpoint interval (30s) | Accepted; documented in product brief |
| Kafka consumer lag grows during peak | Medium | Medium | Scale parallelism via savepoint + redeploy; consumer lag alert at 10k | Platform team runbook |
| Parse errors from upstream schema change | Medium | Low | DLQ captures; `parse_errors` alert; additive-only schema policy | Order Service + this team |
| SMS cost exceeds budget | Low (with approved budget) | High | `sms_commands_emitted` rate alert; Finance dashboards | Finance/Product |
| RocksDB corruption on TM crash | Low | High | Incremental checkpoints on S3; restore from prior checkpoint in < 5 min | Validated in Phase 1 |
| Kafka broker loss | Low | Medium | RF=3, min.ISR=2; Flink auto-reconnects and restarts from checkpoint | Kafka platform team |
| Flink Operator upgrade breaks deployment | Low | Medium | Test on staging first; savepoint before any operator upgrade | DevOps |
| S3 unavailable | Very low | High | Checkpoint fails; Flink waits and retries; job continues running without checkpointing until S3 recovers | Cloud provider SLA |
| SMS service downstream unavailable | Medium | Low | `sms-commands` topic retains 7 days; SMS service catches up on recovery | Notifications team |

---

## Dependencies and Blockers

| Dependency | Owner | Status | Blocking Phase |
|-----------|-------|--------|----------------|
| Kafka cluster (RF=3, 24 partitions) | Platform/Infra | **Needs provisioning** | Phase 0 |
| Flink Operator installation (staging) | DevOps | **Needs installation** | Phase 1 |
| Flink Operator installation (prod) | DevOps | **Needs installation** | Phase 2 |
| S3 bucket with encryption + lifecycle | Infra | **Needs creation** | Phase 0 |
| mTLS configuration for Flink ↔ Kafka | Platform + Security | **Not started** | Phase 1 |
| SMS service `commandId` deduplication | Notifications | **Under development** | Phase 3 |
| Prometheus scrape config + Grafana dashboards | Observability | **Needs setup** | Phase 1 |
| Finance sign-off on SMS budget | Finance | **Pending analysis** | Phase 0 |
| OAuth2 proxy for Flink web UI | DevOps | **Not started** | Phase 1 |
| K8s Network Policies | Security | **Not started** | Phase 1 |
| Container registry | DevOps | Available | — |
| Staging K8s cluster | Platform | Available | — |

---

## Sign-Off Requirements

The following approvals are required before Phase 3 (production cutover):

| Approver | Approval Required For |
|----------|-----------------------|
| Engineering Manager | Overall go-live decision |
| Platform Lead | Infrastructure spec and security controls |
| Finance | SMS budget approval (~$75k–$95k/month) |
| Product Manager | SMS volume estimates and delayed-order-rate assumptions |
| Notifications Team Lead | `commandId` deduplication contract confirmed |
| Security | Network policies, mTLS, Flink UI auth |

---

## Success Criteria

### Technical (Measured During Phase 2 Shadow Mode)

| Metric | Target |
|--------|--------|
| P99 delay-to-SMS-command latency | < 2 minutes |
| Checkpoint success rate | > 99.9% |
| Parse error rate | < 0.01% |
| Job uptime | > 99.9% |
| Consumer lag at peak | < 10,000 messages |

### Business (Measured Month 1–3 Post-Cutover)

| Metric | Baseline | Target (Month 3) |
|--------|----------|-------------------|
| Delay-related support tickets | 2,000/month | < 1,400/month (−30%) |
| CSAT (delivery experience) | 3.8 / 5 | > 4.2 / 5 |
| Time-to-notify (ETA breach → SMS received) | None | < 2 minutes (P99) |
| SMS delivery success rate | N/A | > 95% |

---

## Appendix

### Reference Documents

- [ADR-0001: Processing Time vs Event Time](../docs/adr/0001-processing-time-vs-event-time.md)
- [ADR-0002: SMS Idempotency Strategy](../docs/adr/0002-sms-idempotency-strategy.md)
- [RFC: Delayed Order Detection](../docs/rfc/delayed-order-detection-rfc.md)
- [Production Runbook](../docs/runbooks/production-runbook.md)
- [Failure Test Runbook](../docs/runbooks/failure-test-runbook.md)

### Glossary

| Term | Definition |
|------|------------|
| ETA | Expected Delivery Time (`expectedDeliveryTime` field) |
| DLQ | Dead Letter Queue — Kafka topic for malformed/invalid events |
| TTL | Time To Live — RocksDB state expiry for stuck orders |
| TM | TaskManager — Flink worker process (runs the actual pipeline) |
| JM | JobManager — Flink coordinator (manages tasks, checkpoints, HA) |
| RF | Replication Factor — number of Kafka replica copies |
| ISR | In-Sync Replicas — Kafka replicas that are caught up to the leader |
| CSAT | Customer Satisfaction Score |
| HA | High Availability |
| PDB | PodDisruptionBudget — K8s object preventing simultaneous pod eviction |
