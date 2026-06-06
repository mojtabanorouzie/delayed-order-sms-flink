package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.OpsAlert;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderStatus;
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

class RestaurantBottleneckProcessFunctionTest {

    private static final int STATE_TTL_DAYS = 7;
    private static final int WINDOW_SIZE_SECONDS = 300;   // 5 min
    private static final int ALERT_THRESHOLD_MINUTES = 15;
    private static final int BASELINE_MINUTES = 8;
    private static final String STORE_ID = "store-001";

    private KeyedOneInputStreamOperatorTestHarness<String, OrderState, OpsAlert> harness;

    @BeforeEach
    void setUp() throws Exception {
        RestaurantBottleneckProcessFunction fn = new RestaurantBottleneckProcessFunction(
                STATE_TTL_DAYS, WINDOW_SIZE_SECONDS, ALERT_THRESHOLD_MINUTES, BASELINE_MINUTES, true);
        KeyedProcessOperator<String, OrderState, OpsAlert> operator =
                new KeyedProcessOperator<>(fn);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                OrderState::getStoreId,
                Types.STRING
        );
        harness.open();
    }

    // ──────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────

    private OrderState order(String orderId, OrderStatus status, Instant lastUpdatedAt) {
        OrderState o = new OrderState();
        o.setOrderId(orderId);
        o.setCustomerId("cust-" + orderId);
        o.setStoreId(STORE_ID);
        o.setStatus(status);
        o.setLastUpdatedAt(lastUpdatedAt);
        o.setCreatedAt(Instant.now());
        o.setSchemaVersion(1);
        return o;
    }

    private OrderState order(String orderId, String storeId, OrderStatus status, Instant lastUpdatedAt) {
        OrderState o = order(orderId, status, lastUpdatedAt);
        o.setStoreId(storeId);
        return o;
    }

    // ──────────────────────────────────────────────────────────────────
    // Window timer
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class WindowTimer {

        @Test
        void timerRegisteredOnFirstAcceptedEvent() throws Exception {
            long now = harness.getProcessingTime();
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, Instant.now()), now);

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        }

        @Test
        void noTimerForNonAcceptedEvents() throws Exception {
            long now = harness.getProcessingTime();
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, Instant.now()), now);
            harness.processElement(order("order-2", OrderStatus.DELIVERED, Instant.now()), now);

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
        }

        @Test
        void timerRegisteredOnlyOnceForMultipleAcceptedEvents() throws Exception {
            long now = harness.getProcessingTime();
            Instant t = Instant.now();
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, t), now);
            harness.processElement(order("order-2", OrderStatus.ACCEPTED, t), now);
            harness.processElement(order("order-3", OrderStatus.ACCEPTED, t), now);

            // All three ACCEPTED events in the same window → only 1 timer
            assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        }

        @Test
        void noOutputBeforeTimerFires() throws Exception {
            long now = harness.getProcessingTime();
            Instant accepted = Instant.now();
            Instant pickedUp = accepted.plusSeconds(20 * 60); // 20 min delta

            harness.processElement(order("order-1", OrderStatus.ACCEPTED, accepted), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, pickedUp), now);

            // Timer has not fired yet
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Alert severity
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class AlertSeverity {

        @Test
        void criticalAlertWhenAvgExceedsAlertThreshold() throws Exception {
            long now = harness.getProcessingTime();
            Instant accepted = Instant.now();
            Instant pickedUp = accepted.plusSeconds(20 * 60); // 20 min > 15 min threshold

            harness.processElement(order("order-1", OrderStatus.ACCEPTED, accepted), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, pickedUp), now);

            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            List<StreamRecord<? extends OpsAlert>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            OpsAlert alert = output.get(0).getValue();
            assertThat(alert.getSeverity()).isEqualTo("CRITICAL");
            assertThat(alert.getStoreId()).isEqualTo(STORE_ID);
            assertThat(alert.getAlertType()).isEqualTo("RESTAURANT_QUEUE_BOTTLENECK");
            assertThat(alert.getOrderCount()).isEqualTo(1);
            assertThat(alert.getAvgPickupTimeMinutes()).isGreaterThan(15.0);
        }

        @Test
        void warningAlertWhenAvgBetweenBaselineAndThreshold() throws Exception {
            long now = harness.getProcessingTime();
            Instant accepted = Instant.now();
            Instant pickedUp = accepted.plusSeconds(10 * 60); // 10 min: > 8 baseline, < 15 threshold

            harness.processElement(order("order-1", OrderStatus.ACCEPTED, accepted), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, pickedUp), now);

            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            List<StreamRecord<? extends OpsAlert>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getSeverity()).isEqualTo("WARNING");
        }

        @Test
        void noAlertWhenAvgBelowBaseline() throws Exception {
            long now = harness.getProcessingTime();
            Instant accepted = Instant.now();
            Instant pickedUp = accepted.plusSeconds(5 * 60); // 5 min < 8 min baseline

            harness.processElement(order("order-1", OrderStatus.ACCEPTED, accepted), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, pickedUp), now);

            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void noAlertWhenWindowHasNoCompletedOrders() throws Exception {
            long now = harness.getProcessingTime();
            // ACCEPTED but no PICKED_UP — window fires with 0 completed orders
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, Instant.now()), now);

            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void warningNotCriticalWhenPickupTimeExactlyAtAlertThreshold() throws Exception {
            // avgPickupMs == alertThresholdMs exactly: condition is `> threshold` (strictly greater),
            // so exactly-at-threshold falls through to WARNING.
            long now = harness.getProcessingTime();
            Instant accepted = Instant.now();
            Instant pickedUp = accepted.plusSeconds(ALERT_THRESHOLD_MINUTES * 60L); // exactly 15 min

            harness.processElement(order("order-1", OrderStatus.ACCEPTED, accepted), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, pickedUp), now);
            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getSeverity()).isEqualTo("WARNING");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Pickup time measurement
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class PickupTimeMeasurement {

        @Test
        void pickedUpWithoutAcceptedIsIgnored() throws Exception {
            long now = harness.getProcessingTime();
            // PICKED_UP with no prior ACCEPTED — should not affect window
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, Instant.now()), now);

            assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void averageComputedCorrectlyAcrossMultipleOrders() throws Exception {
            long now = harness.getProcessingTime();
            Instant t0 = Instant.now();

            // order-1: 20 min pickup time
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, t0.plusSeconds(20 * 60)), now);

            // order-2: 10 min pickup time
            harness.processElement(order("order-2", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-2", OrderStatus.PICKED_UP, t0.plusSeconds(10 * 60)), now);

            // avg = 15 min — exactly at threshold, not above it → WARNING (not CRITICAL)
            // alertThresholdMs = 15 * 60 * 1000 = strict >, so avg = 15 is NOT > threshold
            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            List<StreamRecord<? extends OpsAlert>> output = harness.extractOutputStreamRecords();
            // avg = 15 min exactly — not strictly > 15 → no CRITICAL; strictly > 8 → WARNING
            assertThat(output).hasSize(1);
            OpsAlert alert = output.get(0).getValue();
            assertThat(alert.getSeverity()).isEqualTo("WARNING");
            assertThat(alert.getOrderCount()).isEqualTo(2);
            assertThat(alert.getAvgPickupTimeMinutes()).isEqualTo(15.0);
        }

        @Test
        void orderPickedUpBeforeAcceptedDeltaIgnored() throws Exception {
            long now = harness.getProcessingTime();
            Instant t0 = Instant.now();

            // ACCEPTED at t0, PICKED_UP at t0-1min → negative delta → ignored
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, t0.minusSeconds(60)), now);

            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            // Delta was negative → not added to window → 0 completed orders → no alert
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void alertIdContainsStoreIdAndWindowEnd() throws Exception {
            long now = harness.getProcessingTime();
            Instant t0 = Instant.now();

            harness.processElement(order("order-1", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, t0.plusSeconds(20 * 60)), now);

            long windowEnd = now + WINDOW_SIZE_SECONDS * 1000L;
            harness.setProcessingTime(windowEnd);

            OpsAlert alert = harness.extractOutputStreamRecords().get(0).getValue();
            assertThat(alert.getAlertId()).startsWith(STORE_ID + ":");
            assertThat(alert.getAlertId()).endsWith(":BOTTLENECK");
            assertThat(alert.getSchemaVersion()).isEqualTo(1);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Window lifecycle
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class WindowLifecycle {

        @Test
        void windowResetsAfterTimerFires() throws Exception {
            long now = harness.getProcessingTime();
            Instant t0 = Instant.now();

            // First window: 1 order with 20 min pickup → CRITICAL
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, t0.plusSeconds(20 * 60)), now);

            long firstWindowEnd = now + WINDOW_SIZE_SECONDS * 1000L;
            harness.setProcessingTime(firstWindowEnd);

            // First alert
            assertThat(harness.extractOutputStreamRecords()).hasSize(1);

            // Second window: new ACCEPTED event after first timer fires
            Instant t1 = Instant.now();
            harness.processElement(order("order-2", OrderStatus.ACCEPTED, t1), firstWindowEnd);
            harness.processElement(order("order-2", OrderStatus.PICKED_UP, t1.plusSeconds(20 * 60)), firstWindowEnd);

            long secondWindowEnd = firstWindowEnd + WINDOW_SIZE_SECONDS * 1000L;
            harness.setProcessingTime(secondWindowEnd);

            // Total: 2 alerts (one per window)
            assertThat(harness.extractOutputStreamRecords()).hasSize(2);
        }

        @Test
        void ordersFromFirstWindowNotLeakedIntoSecond() throws Exception {
            long now = harness.getProcessingTime();
            Instant t0 = Instant.now();

            // order-1 accepted in window 1 but not picked up in window 1
            harness.processElement(order("order-1", OrderStatus.ACCEPTED, t0), now);

            long firstWindowEnd = now + WINDOW_SIZE_SECONDS * 1000L;
            harness.setProcessingTime(firstWindowEnd);
            // No alert (0 completed orders)
            assertThat(harness.extractOutputStreamRecords()).isEmpty();

            // order-1 picked up in window 2 — delta recorded in window 2
            Instant pickedUp = t0.plusSeconds(20 * 60);
            harness.processElement(order("order-1", OrderStatus.PICKED_UP, pickedUp), firstWindowEnd);

            // New ACCEPTED event to register window 2's timer
            Instant t1 = Instant.now();
            harness.processElement(order("order-2", OrderStatus.ACCEPTED, t1), firstWindowEnd);

            long secondWindowEnd = firstWindowEnd + WINDOW_SIZE_SECONDS * 1000L;
            harness.setProcessingTime(secondWindowEnd);

            // 1 alert from window 2 (order-1's pickup delta was recorded in window 2)
            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
            assertThat(harness.extractOutputStreamRecords().get(0).getValue().getSeverity())
                    .isEqualTo("CRITICAL");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Invalid input handling
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class InvalidInputHandling {

        @Test
        void nullOrderIdRoutesToDeadLetter() throws Exception {
            OrderState bad = order(null, OrderStatus.ACCEPTED, Instant.now());
            harness.processElement(bad, 0);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(RestaurantBottleneckProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void nullStatusRoutesToDeadLetter() throws Exception {
            OrderState bad = order("order-1", null, Instant.now());
            harness.processElement(bad, 0);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(RestaurantBottleneckProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void nullLastUpdatedAtRoutesToDeadLetter() throws Exception {
            OrderState bad = order("order-1", OrderStatus.ACCEPTED, null);
            harness.processElement(bad, 0);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(RestaurantBottleneckProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Multiple stores (independent state)
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class MultipleStores {

        @Test
        void differentStoresHaveIndependentWindows() throws Exception {
            long now = harness.getProcessingTime();
            Instant t0 = Instant.now();

            // store-001: 20 min pickup → CRITICAL
            harness.processElement(order("order-1", "store-001", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-1", "store-001", OrderStatus.PICKED_UP, t0.plusSeconds(20 * 60)), now);

            // store-002: 5 min pickup → no alert
            harness.processElement(order("order-2", "store-002", OrderStatus.ACCEPTED, t0), now);
            harness.processElement(order("order-2", "store-002", OrderStatus.PICKED_UP, t0.plusSeconds(5 * 60)), now);

            harness.setProcessingTime(now + WINDOW_SIZE_SECONDS * 1000L);

            List<StreamRecord<? extends OpsAlert>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getStoreId()).isEqualTo("store-001");
            assertThat(output.get(0).getValue().getSeverity()).isEqualTo("CRITICAL");
        }
    }
}
