package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderStatus;
import com.company.delayedordersms.model.SurgePricingSignal;
import com.company.delayedordersms.model.WeatherData;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DynamicSurgePricingCoProcessFunctionTest {

    private static final String ZONE = "zone-1";
    private static final int WINDOW_SIZE_SECONDS = 30;
    private static final int AT_RISK_THRESHOLD_MINUTES = 25;
    private static final double SURGE_THRESHOLD = 1.15;
    private static final double DEMAND_WEIGHT = 0.5;
    private static final double CHANGE_THRESHOLD = 0.05;
    private static final double MAX_MULTIPLIER = 3.0;

    private KeyedTwoInputStreamOperatorTestHarness<String, OrderState, WeatherData, SurgePricingSignal> harness;

    @BeforeEach
    void setUp() throws Exception {
        DynamicSurgePricingCoProcessFunction fn = new DynamicSurgePricingCoProcessFunction(
                7, WINDOW_SIZE_SECONDS, AT_RISK_THRESHOLD_MINUTES,
                SURGE_THRESHOLD, DEMAND_WEIGHT, CHANGE_THRESHOLD, MAX_MULTIPLIER, true);

        KeyedCoProcessOperator<String, OrderState, WeatherData, SurgePricingSignal> op =
                new KeyedCoProcessOperator<>(fn);

        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                op,
                OrderState::getZoneId,
                WeatherData::getRegion,
                Types.STRING
        );
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    // ──────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────

    /** Order with ETA relative to harness processing time. */
    private OrderState orderWithEta(String zone, String orderId, long etaOffsetMs) {
        long now = harness.getProcessingTime();
        OrderState o = new OrderState();
        o.setOrderId(orderId);
        o.setZoneId(zone);
        o.setStatus(OrderStatus.CREATED);
        o.setExpectedDeliveryTime(Instant.ofEpochMilli(now + etaOffsetMs));
        o.setCreatedAt(Instant.now());
        o.setLastUpdatedAt(Instant.now());
        o.setSchemaVersion(1);
        return o;
    }

    private OrderState atRiskOrder(String orderId) {
        // ETA +30m — above 25 min threshold → at-risk
        return orderWithEta(ZONE, orderId, 30 * 60 * 1_000L);
    }

    private OrderState notAtRiskOrder(String orderId) {
        // ETA +5m — below 25 min threshold → not at-risk
        return orderWithEta(ZONE, orderId, 5 * 60 * 1_000L);
    }

    private WeatherData weather(String region, String condition) {
        WeatherData w = new WeatherData();
        w.setRegion(region);
        w.setCondition(condition);
        w.setTemperature(15.0);
        w.setWindSpeed(10.0);
        w.setTimestamp(Instant.now());
        w.setSchemaVersion(1);
        return w;
    }

    private void sendOrders(int n, boolean atRisk) throws Exception {
        for (int i = 1; i <= n; i++) {
            OrderState o = atRisk ? atRiskOrder("order-" + i) : notAtRiskOrder("order-" + i);
            harness.processElement1(new StreamRecord<>(o, 0L));
        }
    }

    private void fireWindow() throws Exception {
        harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);
    }

    // ──────────────────────────────────────────────────────────────────
    // Surge signal emission
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class SurgeSignalEmission {

        @Test
        void noSignalWhenNoOrdersArriveBeforeWindowFires() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            // No orders → no window timer registered → timer fire at 0 doesn't trigger anything
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void surgeEmittedWithAllAtRiskOrdersAndRainWeather() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            sendOrders(5, true);   // demandFactor = 5/5 = 1.0, weatherFactor = 1.2
            // combined = 1.0 + (1.0 × 0.5) × 1.2 = 1.6 > 1.15 threshold
            fireWindow();

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            SurgePricingSignal signal = output.get(0).getValue();
            assertThat(signal.getSignalType()).isEqualTo("SURGE_PRICING");
            assertThat(signal.getZoneId()).isEqualTo(ZONE);
            assertThat(signal.getWeatherCondition()).isEqualTo("RAIN");
            assertThat(signal.getOrdersInWindow()).isEqualTo(5);
            assertThat(signal.getAtRiskOrderCount()).isEqualTo(5);
            assertThat(signal.getSchemaVersion()).isEqualTo(1);
        }

        @Test
        void noSurgeWhenCombinedMultiplierBelowThreshold() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "CLEAR"), 0L));
            // 1 at-risk out of 5 orders: demandFactor = 0.2, weatherFactor = 0.95
            // combined = 1.0 + (0.2 × 0.5) × 0.95 = 1.095 < 1.15 threshold
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-1"), 0L));
            for (int i = 2; i <= 5; i++) {
                harness.processElement1(new StreamRecord<>(notAtRiskOrder("order-" + i), 0L));
            }
            fireWindow();

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void surgeSignalContainsCorrectCalculatedMultiplier() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            sendOrders(4, true);   // all at-risk: demandFactor = 1.0
            fireWindow();
            // combined = 1.0 + (1.0 × 0.5) × 1.2 = 1.6

            SurgePricingSignal signal = harness.extractOutputStreamRecords().get(0).getValue();
            assertThat(signal.getSurgeMultiplier()).isCloseTo(1.6, within(0.001));
            assertThat(signal.getDemandFactor()).isCloseTo(1.0, within(0.001));
            assertThat(signal.getWeatherFactor()).isCloseTo(1.2, within(0.001));
        }

        @Test
        void surgeEmittedWithDemandOnlyWhenNoWeatherData() throws Exception {
            // No weather injected → weatherFactor defaults to 1.0
            sendOrders(5, true);   // demandFactor = 1.0
            // combined = 1.0 + (1.0 × 0.5) × 1.0 = 1.5 > 1.15 threshold
            fireWindow();

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            SurgePricingSignal signal = output.get(0).getValue();
            assertThat(signal.getWeatherCondition()).isEqualTo("UNKNOWN");
            assertThat(signal.getSurgeMultiplier()).isCloseTo(1.5, within(0.001));
        }

        @Test
        void changeThresholdSuppressesNearDuplicateSignals() throws Exception {
            // changeThreshold = 0.05 (5%)
            // First window: RAIN, 5 at-risk → multiplier = 1.6 → emit
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            sendOrders(5, true);
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            // Second window: same conditions → same multiplier = 1.6
            // |1.6 - 1.6| / max(1.6, 1.0) = 0 < 0.05 → suppressed
            for (int i = 6; i <= 10; i++) {
                harness.processElement1(new StreamRecord<>(atRiskOrder("order-" + i), 0L));
            }
            harness.setProcessingTime((long) WINDOW_SIZE_SECONDS * 1_000 * 2 + 2);

            // Only 1 signal emitted (second suppressed as identical)
            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
        }

        @Test
        void multiplierCappedAtMaxMultiplier() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "SNOW"), 0L));
            sendOrders(5, true);
            // combined_uncapped = 1.0 + (1.0 × 0.5) × 1.4 = 1.7 — below maxMultiplier=3.0
            // No cap triggered here; cap would apply if multiplier exceeded 3.0
            fireWindow();

            SurgePricingSignal signal = harness.extractOutputStreamRecords().get(0).getValue();
            assertThat(signal.getSurgeMultiplier()).isLessThanOrEqualTo(MAX_MULTIPLIER);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Weather factor mapping
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class WeatherFactors {

        @Test
        void rainGives1_2xFactor() {
            double factor = DynamicSurgePricingCoProcessFunction.getWeatherFactor(
                    com.company.delayedordersms.model.WeatherCondition.RAIN);
            assertThat(factor).isCloseTo(1.2, within(0.001));
        }

        @Test
        void snowGives1_4xFactor() {
            double factor = DynamicSurgePricingCoProcessFunction.getWeatherFactor(
                    com.company.delayedordersms.model.WeatherCondition.SNOW);
            assertThat(factor).isCloseTo(1.4, within(0.001));
        }

        @Test
        void clearGives0_95xFactor() {
            double factor = DynamicSurgePricingCoProcessFunction.getWeatherFactor(
                    com.company.delayedordersms.model.WeatherCondition.CLEAR);
            assertThat(factor).isCloseTo(0.95, within(0.001));
        }

        @Test
        void cloudyGivesNeutral1_0Factor() {
            double factor = DynamicSurgePricingCoProcessFunction.getWeatherFactor(
                    com.company.delayedordersms.model.WeatherCondition.CLOUDY);
            assertThat(factor).isCloseTo(1.0, within(0.001));
        }

        @Test
        void unknownWeatherDefaultsToNeutral1_0() {
            double factor = DynamicSurgePricingCoProcessFunction.getWeatherFactor(
                    com.company.delayedordersms.model.WeatherCondition.UNKNOWN);
            assertThat(factor).isCloseTo(1.0, within(0.001));
        }

        @Test
        void snowBoostsMoreThanRain() throws Exception {
            // Run two windows: first with RAIN, second with SNOW
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            sendOrders(5, true);
            fireWindow();  // fires at WINDOW_SIZE_SECONDS * 1000 + 1 = 30001ms
            double rainMultiplier = harness.extractOutputStreamRecords().get(0).getValue().getSurgeMultiplier();

            // Second window: advance 500ms then add orders at 30501ms
            // New orders register timer at 30501 + 30000 = 60501ms
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 501);
            harness.processElement2(new StreamRecord<>(weather(ZONE, "SNOW"), 0L));
            for (int i = 6; i <= 10; i++) {
                harness.processElement1(new StreamRecord<>(atRiskOrder("order-" + i), 0L));
            }
            // Advance past 60501ms to fire the second window timer
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L * 2 + 502);

            // SNOW multiplier change from RAIN: |1.7-1.6|/1.6 = 0.0625 > 0.05 → emits
            List<StreamRecord<? extends SurgePricingSignal>> allOutput = harness.extractOutputStreamRecords();
            assertThat(allOutput).hasSize(2);
            double snowMultiplier = allOutput.get(1).getValue().getSurgeMultiplier();
            assertThat(snowMultiplier).isGreaterThan(rainMultiplier);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Demand factor calculation
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class DemandFactor {

        @Test
        void atRiskOrdersIncrementAtRiskCount() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-1"), 0L));
            harness.processElement1(new StreamRecord<>(notAtRiskOrder("order-2"), 0L));
            harness.processElement1(new StreamRecord<>(notAtRiskOrder("order-3"), 0L));
            // atRisk=1, total=3, demandFactor=0.333
            // combined = 1.0 + (0.333 × 0.5) × 1.2 = 1.2 > 1.15 threshold
            fireWindow();

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            SurgePricingSignal signal = output.get(0).getValue();
            assertThat(signal.getAtRiskOrderCount()).isEqualTo(1);
            assertThat(signal.getOrdersInWindow()).isEqualTo(3);
        }

        @Test
        void noAtRiskOrdersNoDemandSurge() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            // 5 not-at-risk orders: demandFactor = 0/5 = 0
            // combined = 1.0 + (0 × 0.5) × 1.2 = 1.0 < 1.15 threshold
            sendOrders(5, false);
            fireWindow();

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }

        @Test
        void demandFactorComputedAsAtRiskRatio() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            // 3 at-risk, 2 not-at-risk: demandFactor = 3/5 = 0.6
            // combined = 1.0 + (0.6 × 0.5) × 1.2 = 1.36 > 1.15 threshold
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-1"), 0L));
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-2"), 0L));
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-3"), 0L));
            harness.processElement1(new StreamRecord<>(notAtRiskOrder("order-4"), 0L));
            harness.processElement1(new StreamRecord<>(notAtRiskOrder("order-5"), 0L));
            fireWindow();

            SurgePricingSignal signal = harness.extractOutputStreamRecords().get(0).getValue();
            assertThat(signal.getAtRiskOrderCount()).isEqualTo(3);
            assertThat(signal.getOrdersInWindow()).isEqualTo(5);
            assertThat(signal.getDemandFactor()).isCloseTo(0.6, within(0.001));
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Window behaviour
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class WindowBehaviour {

        // These tests verify window reset / isolation mechanics, not change-filtering.
        // Using changeThreshold=0.0 ensures every window that crosses the surge threshold emits.
        @BeforeEach
        void useNoChangeFilter() throws Exception {
            harness.close();
            DynamicSurgePricingCoProcessFunction fn = new DynamicSurgePricingCoProcessFunction(
                    7, WINDOW_SIZE_SECONDS, AT_RISK_THRESHOLD_MINUTES,
                    SURGE_THRESHOLD, DEMAND_WEIGHT, 0.0, MAX_MULTIPLIER, true);
            KeyedCoProcessOperator<String, OrderState, WeatherData, SurgePricingSignal> op =
                    new KeyedCoProcessOperator<>(fn);
            harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                    op, OrderState::getZoneId, WeatherData::getRegion, Types.STRING);
            harness.open();
        }

        @Test
        void windowResetsAfterTimerFires() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));

            // Window 1
            sendOrders(5, true);
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            // Window 2 — new orders after first timer fired
            for (int i = 6; i <= 10; i++) {
                harness.processElement1(new StreamRecord<>(atRiskOrder("order-" + i), 0L));
            }
            harness.setProcessingTime((long) WINDOW_SIZE_SECONDS * 1_000 * 2 + 2);

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(2);
        }

        @Test
        void noSignalWhenWindowFiresWithEmptyDemand() throws Exception {
            // Register a timer by injecting and then completing a window
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));
            sendOrders(5, true);
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1); // fires with data → 1 signal

            // Advance time again with no new orders
            harness.setProcessingTime((long) WINDOW_SIZE_SECONDS * 1_000 * 3 + 1);

            // Still only 1 signal total
            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
        }

        @Test
        void ordersFromDifferentWindowsAreIsolated() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));

            // 2 at-risk orders in window 1
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-1"), 0L));
            harness.processElement1(new StreamRecord<>(atRiskOrder("order-2"), 0L));
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            // 4 at-risk orders in window 2
            for (int i = 3; i <= 6; i++) {
                harness.processElement1(new StreamRecord<>(atRiskOrder("order-" + i), 0L));
            }
            harness.setProcessingTime((long) WINDOW_SIZE_SECONDS * 1_000 * 2 + 2);

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(2);
            // Window 1: 2 orders; Window 2: 4 orders (not cumulative)
            assertThat(output.get(0).getValue().getOrdersInWindow()).isEqualTo(2);
            assertThat(output.get(1).getValue().getOrdersInWindow()).isEqualTo(4);
        }

    }

    // ──────────────────────────────────────────────────────────────────
    // Multiple zones (independent state)
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class MultipleZones {

        @Test
        void zonesHaveIndependentWeatherState() throws Exception {
            // zone-1: RAIN, zone-2: CLEAR
            harness.processElement2(new StreamRecord<>(weather("zone-1", "RAIN"), 0L));
            harness.processElement2(new StreamRecord<>(weather("zone-2", "CLEAR"), 0L));

            // zone-1: 5 at-risk orders → combined = 1.6 > 1.15 → surge
            for (int i = 1; i <= 5; i++) {
                OrderState o = orderWithEta("zone-1", "z1-order-" + i, 30 * 60 * 1_000L);
                harness.processElement1(new StreamRecord<>(o, 0L));
            }
            // zone-2: 1 at-risk, 4 not-at-risk → demandFactor=0.2, weatherFactor=0.95
            // combined = 1.0 + (0.2 × 0.5) × 0.95 = 1.095 < 1.15 → no surge
            OrderState z2at = orderWithEta("zone-2", "z2-order-1", 30 * 60 * 1_000L);
            harness.processElement1(new StreamRecord<>(z2at, 0L));
            for (int i = 2; i <= 5; i++) {
                OrderState z2 = orderWithEta("zone-2", "z2-order-" + i, 5 * 60 * 1_000L);
                harness.processElement1(new StreamRecord<>(z2, 0L));
            }

            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getValue().getZoneId()).isEqualTo("zone-1");
        }

        @Test
        void twoZonesCanBothSurgeIndependently() throws Exception {
            harness.processElement2(new StreamRecord<>(weather("zone-1", "RAIN"), 0L));
            harness.processElement2(new StreamRecord<>(weather("zone-2", "RAIN"), 0L));

            for (int i = 1; i <= 5; i++) {
                harness.processElement1(new StreamRecord<>(
                        orderWithEta("zone-1", "z1-" + i, 30 * 60 * 1_000L), 0L));
                harness.processElement1(new StreamRecord<>(
                        orderWithEta("zone-2", "z2-" + i, 30 * 60 * 1_000L), 0L));
            }

            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            List<StreamRecord<? extends SurgePricingSignal>> output = harness.extractOutputStreamRecords();
            assertThat(output).hasSize(2);
            assertThat(output.stream().map(r -> r.getValue().getZoneId()))
                    .containsExactlyInAnyOrder("zone-1", "zone-2");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Invalid input handling
    // ──────────────────────────────────────────────────────────────────

    @Nested
    class InvalidInputHandling {

        @Test
        void nullOrderIdRoutesToDeadLetter() throws Exception {
            OrderState bad = orderWithEta(ZONE, null, 30 * 60 * 1_000L);
            harness.processElement1(new StreamRecord<>(bad, 0L));

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            assertThat(harness.getSideOutput(DynamicSurgePricingCoProcessFunction.INVALID_ORDER_TAG))
                    .hasSize(1);
        }

        @Test
        void nullStatusRoutesToDeadLetter() throws Exception {
            OrderState bad = orderWithEta(ZONE, "order-1", 30 * 60 * 1_000L);
            bad.setStatus(null);
            harness.processElement1(new StreamRecord<>(bad, 0L));

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
            assertThat(harness.getSideOutput(DynamicSurgePricingCoProcessFunction.INVALID_ORDER_TAG))
                    .hasSize(1);
        }

        @Test
        void validOrdersNotAffectedByPrecedingInvalidOne() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));

            OrderState bad = orderWithEta(ZONE, null, 30 * 60 * 1_000L);
            harness.processElement1(new StreamRecord<>(bad, 0L));  // invalid — goes to DLQ

            sendOrders(5, true);  // valid orders
            fireWindow();

            assertThat(harness.extractOutputStreamRecords()).hasSize(1);
        }

        @Test
        void nonCreatedStatusOrdersDoNotContributeToDemandWindow() throws Exception {
            harness.processElement2(new StreamRecord<>(weather(ZONE, "RAIN"), 0L));

            // DELIVERED and CANCELLED events should not accumulate in the demand window
            for (int i = 1; i <= 5; i++) {
                OrderState o = orderWithEta(ZONE, "order-" + i, 30 * 60 * 1_000L);
                o.setStatus(i % 2 == 0 ? OrderStatus.DELIVERED : OrderStatus.CANCELLED);
                harness.processElement1(new StreamRecord<>(o, 0L));
            }
            // No window timer registered → timer advance does nothing
            harness.setProcessingTime(WINDOW_SIZE_SECONDS * 1_000L + 1);

            assertThat(harness.extractOutputStreamRecords()).isEmpty();
        }
    }
}
