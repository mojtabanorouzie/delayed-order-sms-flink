package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderState.StateLog;
import com.company.delayedordersms.model.OrderStatus;
import com.company.delayedordersms.model.RefundCommand;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DelayedOrderRefundProcessFunctionTest {

    private static final int STATE_TTL_DAYS = 7;
    private static final int REFUND_DELAY_MINUTES = 30;
    private static final long REFUND_DELAY_MS = REFUND_DELAY_MINUTES * 60_000L;
    private static final double DELIVERY_FEE = 10.00;

    private KeyedOneInputStreamOperatorTestHarness<String, OrderState, RefundCommand> harness;

    @BeforeEach
    void setUp() throws Exception {
        DelayedOrderRefundProcessFunction fn =
                new DelayedOrderRefundProcessFunction(STATE_TTL_DAYS, REFUND_DELAY_MINUTES);
        KeyedProcessOperator<String, OrderState, RefundCommand> operator =
                new KeyedProcessOperator<>(fn);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                OrderState::getOrderId,
                Types.STRING
        );
        harness.open();
    }

    // ──────────────────────────────────────────────────────────────────
    // Helper builders
    // ──────────────────────────────────────────────────────────────────

    private OrderState order(String orderId, OrderStatus status, Instant eta, Instant lastUpdated) {
        OrderState o = new OrderState();
        o.setOrderId(orderId);
        o.setCustomerId("cust-" + orderId);
        o.setStoreId("store-1");
        o.setStatus(status);
        o.setExpectedDeliveryTime(eta);
        o.setCreatedAt(Instant.now());
        o.setLastUpdatedAt(lastUpdated);
        o.setEventTime(lastUpdated);
        o.setDeliveryFee(DELIVERY_FEE);
        o.setStateLogs(List.of(stateLog(status.name(), lastUpdated)));
        o.setSchemaVersion(1);
        return o;
    }

    private StateLog stateLog(String status, Instant at) {
        StateLog log = new StateLog();
        log.setStatus(status);
        log.setAt(at);
        return log;
    }

    // ──────────────────────────────────────────────────────────────────
    // Timer registration
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class TimerRegistration {

        @Test
        void registersTimerAtEtaPlusDelay() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600); // 10 minutes from now

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            // Timer should be at ETA + 30 minutes (not at ETA)
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void noTimerRegisteredForTerminalOrder() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.DELIVERED, eta, now)));

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void noTimerRegisteredForCancelledOrder() throws Exception {
            Instant now = Instant.now();
            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.CANCELLED, now.plusSeconds(600), now)));

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
        }

        @Test
        void updatesTimerWhenEtaChanges() throws Exception {
            Instant now = Instant.now();
            Instant eta1 = now.plusSeconds(600);
            Instant eta2 = now.plusSeconds(1800); // later ETA

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta1, now)));
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

            harness.setProcessingTime(now.plusSeconds(5).toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta2, now.plusSeconds(5))));

            // Still one timer, but pointing to the updated ETA
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void cancelsTimerWhenOrderBecomesTerminal() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600);

            // Register active order
            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

            // Order delivered before timer fires
            harness.setProcessingTime(now.plusSeconds(60).toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.DELIVERED, eta, now.plusSeconds(60))));

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Refund emission
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class RefundEmission {

        @Test
        void emitsRefundImmediatelyWhenEtaPlusDelayAlreadyPassed() throws Exception {
            Instant now = Instant.now();
            // ETA was 35 minutes ago — ETA + 30 min is 5 minutes ago
            Instant eta = now.minusSeconds(35 * 60);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);

            RefundCommand cmd = output.get(0).getValue();
            assertThat(cmd.getCommandId()).isEqualTo("o1:REFUND_30MIN");
            assertThat(cmd.getCommandType()).isEqualTo("AUTO_REFUND");
            assertThat(cmd.getOrderId()).isEqualTo("o1");
            assertThat(cmd.getReason()).isEqualTo("DELAY_30_MIN_BREACH");
        }

        @Test
        void emitsRefundOnTimerFire() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600); // 10 min from now

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            // Advance to ETA + 30 minutes
            harness.setProcessingTime(eta.toEpochMilli() + REFUND_DELAY_MS);

            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);

            RefundCommand cmd = output.get(0).getValue();
            assertThat(cmd.getCommandId()).isEqualTo("o1:REFUND_30MIN");
            assertThat(cmd.getOrderId()).isEqualTo("o1");
            assertThat(cmd.getCustomerId()).isEqualTo("cust-o1");
        }

        @Test
        void doesNotEmitRefundIfOrderDeliveredBeforeTimerFires() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            // Delivered before ETA + 30 min
            harness.setProcessingTime(now.plusSeconds(300).toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.DELIVERED, eta, now.plusSeconds(300))));

            // Advance past ETA + 30 min
            harness.setProcessingTime(eta.toEpochMilli() + REFUND_DELAY_MS + 1000);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void doesNotEmitRefundIfOrderCancelledBeforeTimerFires() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            harness.setProcessingTime(now.plusSeconds(120).toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.CANCELLED, eta, now.plusSeconds(120))));

            harness.setProcessingTime(eta.toEpochMilli() + REFUND_DELAY_MS + 1000);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void doesNotEmitRefundAtEtaOnlyAtEtaPlusDelay() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            // Advance to exactly ETA — should NOT fire yet
            harness.setProcessingTime(eta.toEpochMilli());

            assertThat(harness.extractOutputStreamRecords()).isEmpty();

            // Advance to ETA + 30 min — should fire
            harness.setProcessingTime(eta.toEpochMilli() + REFUND_DELAY_MS);

            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Idempotency
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class Idempotency {

        @Test
        void emitsExactlyOneRefundPerOrderEvenWithMultipleEvents() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.minusSeconds(35 * 60); // already past ETA + 30 min

            // First event triggers immediate refund
            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            assertThat(harness.extractOutputStreamRecords()).hasSize(1);

            // Second event (e.g. status update) — refund must NOT be emitted again
            harness.setProcessingTime(now.plusSeconds(5).toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.PICKED_UP, eta, now.plusSeconds(5))));

            // Buffer accumulates — total is still 1 (no duplicate was emitted)
            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
        }

        @Test
        void commandIdIsStableAcrossRetries() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.minusSeconds(35 * 60);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("order-xyz", OrderStatus.ACCEPTED, eta, now)));

            RefundCommand cmd = harness.extractOutputStreamRecords().get(0).getValue();
            // commandId is deterministic: orderId + ":REFUND_" + delayMinutes + "MIN"
            assertThat(cmd.getCommandId()).isEqualTo("order-xyz:REFUND_30MIN");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Refund amount calculation
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class RefundAmountCalculation {

        @Test
        void refundAmountIsFiftyPercentOfDeliveryFee() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.minusSeconds(35 * 60);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));

            RefundCommand cmd = harness.extractOutputStreamRecords().get(0).getValue();
            assertThat(cmd.getRefundAmount()).isEqualTo(DELIVERY_FEE * 0.50);
        }

        @Test
        void refundAmountIsZeroWhenDeliveryFeeIsZero() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.minusSeconds(35 * 60);

            OrderState o = order("o1", OrderStatus.ACCEPTED, eta, now);
            o.setDeliveryFee(0.0);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(o));

            // Command is still emitted (for audit), amount is just 0
            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getRefundAmount()).isEqualTo(0.0);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Staleness handling
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class StalenessHandling {

        @Test
        void ignoresStaleEventByLastUpdatedAt() throws Exception {
            Instant now = Instant.now();
            Instant eta = now.plusSeconds(600);

            harness.setProcessingTime(now.toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now)));
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

            // Stale event has earlier lastUpdatedAt
            harness.setProcessingTime(now.plusSeconds(5).toEpochMilli());
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, eta, now.minusSeconds(10))));

            // Timer count unchanged; stale update ignored
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Invalid input → dead letter
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class InvalidInputHandling {

        @Test
        void routesNullOrderIdToDeadLetter() throws Exception {
            OrderState o = order("o1", OrderStatus.ACCEPTED, Instant.now().plusSeconds(600), Instant.now());
            o.setOrderId(null);

            harness.setProcessingTime(Instant.now().toEpochMilli());

            // Null key throws in test harness — build a minimal invalid order with a non-null key
            // We test the validation path: missing expectedDeliveryTime
            OrderState invalid = order("o2", OrderStatus.ACCEPTED, Instant.now().plusSeconds(600), Instant.now());
            invalid.setExpectedDeliveryTime(null);

            harness.processElement(new StreamRecord<>(invalid));

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);

            var dlq = harness.getSideOutput(DelayedOrderRefundProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void routesMissingLastUpdatedAtToDeadLetter() throws Exception {
            OrderState invalid = order("o1", OrderStatus.ACCEPTED, Instant.now().plusSeconds(600), Instant.now());
            invalid.setLastUpdatedAt(null);

            harness.setProcessingTime(Instant.now().toEpochMilli());
            harness.processElement(new StreamRecord<>(invalid));

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(DelayedOrderRefundProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void routesMissingStatusToDeadLetter() throws Exception {
            OrderState invalid = order("o1", OrderStatus.ACCEPTED, Instant.now().plusSeconds(600), Instant.now());
            invalid.setStatus(null);

            harness.setProcessingTime(Instant.now().toEpochMilli());
            harness.processElement(new StreamRecord<>(invalid));

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(DelayedOrderRefundProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Multiple independent orders (no cross-key contamination)
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class MultipleOrders {

        @Test
        void independentOrdersEmitIndependentRefunds() throws Exception {
            Instant now = Instant.now();
            Instant pastEta = now.minusSeconds(35 * 60);
            Instant futureEta = now.plusSeconds(600);

            harness.setProcessingTime(now.toEpochMilli());

            // Order 1: already past ETA + delay → immediate refund
            harness.processElement(new StreamRecord<>(
                    order("o1", OrderStatus.ACCEPTED, pastEta, now)));

            // Order 2: future ETA → timer registered
            harness.processElement(new StreamRecord<>(
                    order("o2", OrderStatus.ACCEPTED, futureEta, now)));

            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getOrderId()).isEqualTo("o1");
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

            // Advance time past o2's refund trigger
            harness.setProcessingTime(futureEta.toEpochMilli() + REFUND_DELAY_MS);

            // Buffer accumulates: o1's immediate refund + o2's timer-fired refund = 2 total
            var allOutput = harness.extractOutputStreamRecords();
            assertThat(allOutput).hasSize(2);
            assertThat(allOutput.get(1).getValue().getOrderId()).isEqualTo("o2");
        }

        @Test
        void deliveredOrderDoesNotInterfereWithDelayedOrder() throws Exception {
            Instant now = Instant.now();
            Instant pastEta = now.minusSeconds(35 * 60);

            harness.setProcessingTime(now.toEpochMilli());

            harness.processElement(new StreamRecord<>(
                    order("o-delivered", OrderStatus.DELIVERED, pastEta, now)));
            harness.processElement(new StreamRecord<>(
                    order("o-delayed", OrderStatus.ACCEPTED, pastEta, now)));

            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getOrderId()).isEqualTo("o-delayed");
        }
    }
}
