package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderStatus;
import com.company.delayedordersms.model.SmsCommand;
import com.company.delayedordersms.model.OrderState.StateLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DelayedOrderProcessFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, OrderState, SmsCommand> harness;

    @BeforeEach
    void setUp() throws Exception {
        DelayedOrderProcessFunction processFunction = new DelayedOrderProcessFunction(7);
        KeyedProcessOperator<String, OrderState, SmsCommand> operator =
                new KeyedProcessOperator<>(processFunction);
        harness = new KeyedOneInputStreamOperatorTestHarness<String, OrderState, SmsCommand>(
                operator,
                OrderState::getOrderId,
                Types.STRING
        );
        harness.open();
    }

    private OrderState createOrder(
            String orderId, OrderStatus status, Instant eta, Instant lastUpdatedAt) {
        OrderState order = new OrderState();
        order.setOrderId(orderId);
        order.setCustomerId("cust-" + orderId);
        order.setStoreId("store-1");
        order.setStatus(status);
        order.setExpectedDeliveryTime(eta);
        order.setCreatedAt(Instant.now());
        order.setLastUpdatedAt(lastUpdatedAt != null ? lastUpdatedAt : Instant.now());
        order.setEventTime(Instant.now());
        order.setStateLogs(List.of(
                createStateLog(status.name(), Instant.now())
        ));
        order.setSchemaVersion(1);
        return order;
    }

    private StateLog createStateLog(String status, Instant at) {
        StateLog log = new StateLog();
        log.setStatus(status);
        log.setAt(at);
        return log;
    }

    @Test
    void shouldRegisterTimerWhenOrderHasFutureEta() throws Exception {
        Instant now = Instant.now();
        Instant futureEta = now.plusSeconds(600); // 10 minutes from now

        OrderState order = createOrder("order-1", OrderStatus.ACCEPTED, futureEta, Instant.now());

        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order));

        // Should register a timer at the ETA
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(harness.numEventTimeTimers()).isEqualTo(0);

        // No output yet
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
    }

    @Test
    void shouldEmitSmsWhenEtaAlreadyPassed() throws Exception {
        Instant now = Instant.now();
        Instant pastEta = now.minusSeconds(60); // 1 minute ago

        OrderState order = createOrder("order-1", OrderStatus.ACCEPTED, pastEta, Instant.now());

        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order));

        // Should emit SmsCommand immediately
        var output = harness.extractOutputStreamRecords();
        assertThat(output).hasSize(1);
        SmsCommand cmd = output.get(0).getValue();
        assertThat(cmd.getCommandId()).isEqualTo("order-1:DELAY_SMS");
        assertThat(cmd.getCommandType()).isEqualTo("SEND_DELAY_SMS");
    }

    @Test
    void shouldNotEmitSmsWhenOrderDelivered() throws Exception {
        Instant now = Instant.now();
        Instant pastEta = now.minusSeconds(60);

        OrderState order = createOrder("order-1", OrderStatus.DELIVERED, pastEta, Instant.now());

        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order));

        // No output
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
        // No timers registered for terminal state
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void shouldNotEmitSmsWhenOrderCancelled() throws Exception {
        Instant now = Instant.now();
        Instant pastEta = now.minusSeconds(60);

        OrderState order = createOrder("order-1", OrderStatus.CANCELLED, pastEta, Instant.now());

        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order));

        // No output
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void shouldNotDuplicateSms() throws Exception {
        Instant now = Instant.now();
        Instant pastEta = now.minusSeconds(60);

        // First event: already past ETA, should emit SMS
        OrderState order1 = createOrder("order-1", OrderStatus.ACCEPTED, pastEta, now);
        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order1));

        var firstOutput = harness.extractOutputStreamRecords();
        assertThat(firstOutput).hasSize(1);

        // Second event: same order with slightly later timestamp
        OrderState order2 = createOrder("order-1", OrderStatus.PICKED_UP, pastEta, now.plusSeconds(1));
        harness.setProcessingTime(now.plusSeconds(1).toEpochMilli());
        harness.processElement(new StreamRecord<>(order2));

        // No additional output (SMS already emitted) — total output still 1
        var allOutput = harness.extractOutputStreamRecords();
        assertThat(allOutput).hasSize(1);
    }

    @Test
    void shouldIgnoreStaleUpdate() throws Exception {
        Instant now = Instant.now();
        Instant futureEta = now.plusSeconds(600);

        // First event with current timestamp
        OrderState order1 = createOrder("order-1", OrderStatus.ACCEPTED, futureEta, now);
        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order1));

        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

        // Stale event with older timestamp
        OrderState order2 = createOrder("order-1", OrderStatus.ACCEPTED, futureEta, now.minusSeconds(300));
        harness.setProcessingTime(now.plusSeconds(10).toEpochMilli());
        harness.processElement(new StreamRecord<>(order2));

        // Still only one timer, no output
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
    }

    @Test
    void shouldUpdateTimerWhenEtaChanges() throws Exception {
        Instant now = Instant.now();
        Instant futureEta1 = now.plusSeconds(600);
        Instant futureEta2 = now.plusSeconds(1200);

        // First event
        OrderState order1 = createOrder("order-1", OrderStatus.ACCEPTED, futureEta1, now);
        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order1));

        long firstTimerCount = harness.numProcessingTimeTimers();

        // Updated event with later ETA
        OrderState order2 = createOrder("order-1", OrderStatus.ACCEPTED, futureEta2, now.plusSeconds(5));
        harness.setProcessingTime(now.plusSeconds(5).toEpochMilli());
        harness.processElement(new StreamRecord<>(order2));

        // Timer should be updated
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
    }

    @Test
    void shouldSkipInvalidOrder() throws Exception {
        Instant now = Instant.now();
        Instant pastEta = now.minusSeconds(60);

        // Create order with valid key but invalid payload (missing lastUpdatedAt)
        // The harness rejects null keys, so the key must be non-null
        OrderState order = createOrder("invalid-1", OrderStatus.ACCEPTED, pastEta, null);
        order.setLastUpdatedAt(null);

        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order));

        // No output, no timers, no crash
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(0);
    }

    @Test
    void shouldSkipTimerFireForTerminalOrder() throws Exception {
        Instant now = Instant.now();
        Instant futureEta = now.plusSeconds(600);

        // Register with active order
        OrderState order1 = createOrder("order-1", OrderStatus.ACCEPTED, futureEta, now);
        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order1));

        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

        // Update to terminal before timer fires
        OrderState order2 = createOrder("order-1", OrderStatus.DELIVERED, futureEta, now.plusSeconds(10));
        harness.setProcessingTime(now.plusSeconds(10).toEpochMilli());
        harness.processElement(new StreamRecord<>(order2));

        // Advance time past ETA
        harness.setProcessingTime(futureEta.toEpochMilli() + 1000);

        // No SMS should be emitted
        assertThat(harness.extractOutputStreamRecords()).isEmpty();
    }

    @Test
    void shouldEmitSmsOnTimerFire() throws Exception {
        Instant now = Instant.now();
        Instant eta = now.plusSeconds(600);

        // Register with active order and future ETA
        OrderState order = createOrder("order-1", OrderStatus.ACCEPTED, eta, now);
        harness.setProcessingTime(now.toEpochMilli());
        harness.processElement(new StreamRecord<>(order));

        // Advance time to ETA
        harness.setProcessingTime(eta.toEpochMilli());

        // SMS should be emitted when timer fires
        var output = harness.extractOutputStreamRecords();
        assertThat(output).hasSize(1);
        SmsCommand cmd = output.get(0).getValue();
        assertThat(cmd.getCommandId()).isEqualTo("order-1:DELAY_SMS");
        assertThat(cmd.getOrderId()).isEqualTo("order-1");
        assertThat(cmd.getReason()).isEqualTo("ORDER_DELAYED");
    }
}
