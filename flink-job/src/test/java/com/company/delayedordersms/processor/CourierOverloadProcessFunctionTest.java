package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.CourierCommand;
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

class CourierOverloadProcessFunctionTest {

    private static final int STATE_TTL_DAYS = 7;
    private static final int OVERLOAD_THRESHOLD = 8;
    private static final int RESUME_THRESHOLD = 5;
    private static final String COURIER_ID = "courier-001";

    private KeyedOneInputStreamOperatorTestHarness<String, OrderState, CourierCommand> harness;

    @BeforeEach
    void setUp() throws Exception {
        CourierOverloadProcessFunction fn = new CourierOverloadProcessFunction(
                STATE_TTL_DAYS, OVERLOAD_THRESHOLD, RESUME_THRESHOLD, true);
        KeyedProcessOperator<String, OrderState, CourierCommand> operator =
                new KeyedProcessOperator<>(fn);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                OrderState::getCourierId,
                Types.STRING
        );
        harness.open();
    }

    // ──────────────────────────────────────────────────────────────────
    // Helper builders
    // ──────────────────────────────────────────────────────────────────

    private OrderState order(String orderId, String courierId, OrderStatus status) {
        OrderState o = new OrderState();
        o.setOrderId(orderId);
        o.setCustomerId("cust-" + orderId);
        o.setStoreId("store-001");
        o.setCourierId(courierId);
        o.setStatus(status);
        o.setLastUpdatedAt(Instant.now());
        o.setCreatedAt(Instant.now());
        o.setSchemaVersion(1);
        return o;
    }

    private void sendN(int n, OrderStatus status) throws Exception {
        for (int i = 1; i <= n; i++) {
            harness.processElement(order("order-" + i, COURIER_ID, status), 0);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Pause detection
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class PauseDetection {

        @Test
        void noPauseWhenBelowThreshold() throws Exception {
            sendN(7, OrderStatus.CREATED);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void pauseEmittedAtExactlyThreshold() throws Exception {
            sendN(8, OrderStatus.CREATED);

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            CourierCommand cmd = output.get(0).getValue();
            assertThat(cmd.getAction()).isEqualTo("PAUSE");
            assertThat(cmd.getCourierId()).isEqualTo(COURIER_ID);
            assertThat(cmd.getActiveOrderCount()).isEqualTo(8);
            assertThat(cmd.getCommandId()).isEqualTo(COURIER_ID + ":PAUSE");
            assertThat(cmd.getCommandType()).isEqualTo("COURIER_PAUSE");
        }

        @Test
        void pauseEmittedAboveThreshold() throws Exception {
            sendN(10, OrderStatus.CREATED);

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
        }

        @Test
        void noDuplicatePauseAfterAlreadyPaused() throws Exception {
            sendN(8, OrderStatus.CREATED);
            // 9th order arrives — courier already paused, no additional command
            harness.processElement(order("order-9", COURIER_ID, OrderStatus.CREATED), 0);
            harness.processElement(order("order-10", COURIER_ID, OrderStatus.CREATED), 0);

            // Total output is still exactly 1 PAUSE
            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
        }

        @Test
        void pauseCommandContainsSchemaVersion1() throws Exception {
            sendN(8, OrderStatus.CREATED);

            CourierCommand cmd = harness.extractOutputStreamRecords().get(0).getValue();
            assertThat(cmd.getSchemaVersion()).isEqualTo(1);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Resume detection
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class ResumeDetection {

        @Test
        void resumeEmittedWhenCountDropsBelowResumeThreshold() throws Exception {
            // Pause first: 8 active orders
            sendN(8, OrderStatus.CREATED);

            // Deliver 4 orders → count drops to 4 (< resumeThreshold=5)
            for (int i = 1; i <= 4; i++) {
                harness.processElement(order("order-" + i, COURIER_ID, OrderStatus.DELIVERED), 0);
            }

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(2);
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
            assertThat(output.get(1).getValue().getAction()).isEqualTo("RESUME");
            assertThat(output.get(1).getValue().getActiveOrderCount()).isEqualTo(4);
            assertThat(output.get(1).getValue().getCommandId()).isEqualTo(COURIER_ID + ":RESUME");
        }

        @Test
        void noResumeWhenCountDropsToExactlyResumeThreshold() throws Exception {
            sendN(8, OrderStatus.CREATED);

            // Deliver 3 → count = 5 (= resumeThreshold, NOT below it)
            for (int i = 1; i <= 3; i++) {
                harness.processElement(order("order-" + i, COURIER_ID, OrderStatus.DELIVERED), 0);
            }

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            // Only PAUSE, no RESUME
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
        }

        @Test
        void noResumeWhenNotPaused() throws Exception {
            // 3 active orders — never reached overload, so not paused
            sendN(3, OrderStatus.CREATED);
            // Deliver them all → count = 0, but never paused
            for (int i = 1; i <= 3; i++) {
                harness.processElement(order("order-" + i, COURIER_ID, OrderStatus.DELIVERED), 0);
            }
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void noDuplicateResumeAfterAlreadyResumed() throws Exception {
            sendN(8, OrderStatus.CREATED);

            // Deliver 4 → triggers RESUME
            for (int i = 1; i <= 4; i++) {
                harness.processElement(order("order-" + i, COURIER_ID, OrderStatus.DELIVERED), 0);
            }
            // Deliver 2 more → count = 2, still not paused, so no second RESUME
            harness.processElement(order("order-5", COURIER_ID, OrderStatus.DELIVERED), 0);
            harness.processElement(order("order-6", COURIER_ID, OrderStatus.DELIVERED), 0);

            assertThat(harness.extractOutputStreamRecords()).hasSize(2); // 1 PAUSE + 1 RESUME
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Terminal order handling
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class TerminalOrderHandling {

        @Test
        void deliveredOrderNotCountedAsActive() throws Exception {
            sendN(7, OrderStatus.CREATED);
            // 8th order arrives and is immediately delivered
            harness.processElement(order("order-8", COURIER_ID, OrderStatus.DELIVERED), 0);
            // Active count = 7 (not 8), so no PAUSE
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void cancelledOrderNotCountedAsActive() throws Exception {
            sendN(7, OrderStatus.CREATED);
            harness.processElement(order("order-8", COURIER_ID, OrderStatus.CANCELLED), 0);
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void pickedUpOrderCountsAsActive() throws Exception {
            // PICKED_UP is not terminal — still counts as active
            sendN(7, OrderStatus.CREATED);
            harness.processElement(order("order-8", COURIER_ID, OrderStatus.PICKED_UP), 0);

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Idempotency
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class Idempotency {

        @Test
        void sameOrderIdUpdatedMultipleTimesCountsOnce() throws Exception {
            sendN(7, OrderStatus.CREATED);
            // order-1 updated multiple times — still only 1 entry for order-1 in the map
            harness.processElement(order("order-1", COURIER_ID, OrderStatus.ACCEPTED), 0);
            harness.processElement(order("order-1", COURIER_ID, OrderStatus.ACCEPTED), 0);
            harness.processElement(order("order-1", COURIER_ID, OrderStatus.CREATED), 0);

            // Count = 7 (orders 1-7, order-1 updated but still same entry)
            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void sameOrderIdEventuallyDeliveredReducesCountOnce() throws Exception {
            sendN(8, OrderStatus.CREATED);
            // Deliver order-1 multiple times → should only remove once
            harness.processElement(order("order-1", COURIER_ID, OrderStatus.DELIVERED), 0);
            harness.processElement(order("order-1", COURIER_ID, OrderStatus.DELIVERED), 0);

            // Count = 7 after first delivery, still paused (7 >= resumeThreshold=5, not below)
            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1); // Only PAUSE, no spurious RESUME
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Invalid input handling
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class InvalidInputHandling {

        @Test
        void blankCourierIdRoutesToDeadLetter() throws Exception {
            OrderState bad = order("order-1", "   ", OrderStatus.CREATED);
            harness.processElement(bad, 0);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(CourierOverloadProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void nullOrderIdRoutesToDeadLetter() throws Exception {
            OrderState bad = order(null, COURIER_ID, OrderStatus.CREATED);
            harness.processElement(bad, 0);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(CourierOverloadProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void nullStatusRoutesToDeadLetter() throws Exception {
            OrderState bad = order("order-1", COURIER_ID, null);
            harness.processElement(bad, 0);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            var dlq = harness.getSideOutput(CourierOverloadProcessFunction.INVALID_ORDER_TAG);
            assertThat(dlq).hasSize(1);
        }

        @Test
        void validOrdersNotAffectedByPrecedingInvalidOne() throws Exception {
            harness.processElement(order(null, COURIER_ID, OrderStatus.CREATED), 0); // invalid
            sendN(8, OrderStatus.CREATED);

            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
            assertThat(harness.extractOutputStreamRecords().get(0).getValue().getAction())
                    .isEqualTo("PAUSE");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Multiple couriers (independent state)
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class MultipleCouriers {

        @Test
        void overloadOnOneCourierDoesNotAffectAnother() throws Exception {
            // courier-001: 8 orders → PAUSE
            sendN(8, OrderStatus.CREATED);

            // courier-002: 3 orders → no PAUSE
            for (int i = 1; i <= 3; i++) {
                harness.processElement(order("c2-order-" + i, "courier-002", OrderStatus.CREATED), 0);
            }

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getCourierId()).isEqualTo(COURIER_ID);
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
        }

        @Test
        void twoCouriersCanBothBePausedIndependently() throws Exception {
            // Both couriers get 8 orders each
            for (int i = 1; i <= 8; i++) {
                harness.processElement(order("c1-order-" + i, "courier-001", OrderStatus.CREATED), 0);
                harness.processElement(order("c2-order-" + i, "courier-002", OrderStatus.CREATED), 0);
            }

            List<StreamRecord<? extends CourierCommand>> output = harness.extractOutputStreamRecords();
            long pauseCount = output.stream()
                    .filter(r -> "PAUSE".equals(r.getValue().getAction()))
                    .count();
            assertThat(pauseCount).isEqualTo(2);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Custom thresholds
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class CustomThresholds {

        @Test
        void shouldRespectCustomOverloadAndResumeThresholds() throws Exception {
            // Harness uses 8/5 defaults. Create a separate function with 4/2.
            CourierOverloadProcessFunction fn = new CourierOverloadProcessFunction(7, 4, 2, true);
            KeyedProcessOperator<String, OrderState, CourierCommand> operator =
                    new KeyedProcessOperator<>(fn);
            KeyedOneInputStreamOperatorTestHarness<String, OrderState, CourierCommand> customHarness =
                    new KeyedOneInputStreamOperatorTestHarness<>(
                            operator, OrderState::getCourierId, Types.STRING);
            customHarness.open();

            // 3 orders — below threshold of 4 → no output
            for (int i = 1; i <= 3; i++) {
                customHarness.processElement(order("order-" + i, COURIER_ID, OrderStatus.CREATED), 0);
            }
            // 4th order → PAUSE. Deliver 3 orders → count=1 < resumeThreshold=2 → RESUME.
            customHarness.processElement(order("order-4", COURIER_ID, OrderStatus.CREATED), 0);
            for (int i = 1; i <= 3; i++) {
                customHarness.processElement(order("order-" + i, COURIER_ID, OrderStatus.DELIVERED), 0);
            }

            // extractOutputStreamRecords accumulates; expect PAUSE then RESUME
            var allOutput = customHarness.extractOutputStreamRecords();
            assertThat(allOutput).hasSize(2);
            assertThat(allOutput.get(0).getValue().getAction()).isEqualTo("PAUSE");
            assertThat(allOutput.get(1).getValue().getAction()).isEqualTo("RESUME");

            customHarness.close();
        }

        @Test
        void shouldResumeWhenAllActiveOrdersCompleted() throws Exception {
            // PAUSE at 8, then all 8 delivered → count drops below resumeThreshold=5 → RESUME
            sendN(8, OrderStatus.CREATED);
            for (int i = 1; i <= 8; i++) {
                harness.processElement(order("order-" + i, COURIER_ID, OrderStatus.DELIVERED), 0);
            }

            // extractOutputStreamRecords accumulates: PAUSE (at 8th CREATED) + RESUME (when count < 5)
            var output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(2);
            assertThat(output.get(0).getValue().getAction()).isEqualTo("PAUSE");
            assertThat(output.get(1).getValue().getAction()).isEqualTo("RESUME");
            assertThat(output.get(1).getValue().getActiveOrderCount()).isLessThan(RESUME_THRESHOLD);
        }
    }
}
