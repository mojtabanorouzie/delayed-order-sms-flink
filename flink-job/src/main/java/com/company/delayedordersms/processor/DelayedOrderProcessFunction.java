package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderDelayState;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.SmsCommand;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

public class DelayedOrderProcessFunction extends KeyedProcessFunction<String, OrderState, SmsCommand> {

    public static final OutputTag<DeadLetterEvent> INVALID_ORDER_TAG =
            new OutputTag<DeadLetterEvent>("invalid-order") {};

    private final int stateTtlDays;

    private transient ValueState<OrderDelayState> orderState;

    private transient Counter delayedOrdersDetected;
    private transient Counter smsCommandsEmitted;
    private transient Counter staleUpdatesIgnored;
    private transient Counter invalidOrders;

    public DelayedOrderProcessFunction(int stateTtlDays) {
        this.stateTtlDays = stateTtlDays;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        ValueStateDescriptor<OrderDelayState> descriptor =
                new ValueStateDescriptor<>("order-delay-state", OrderDelayState.class);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(stateTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        descriptor.enableTimeToLive(ttlConfig);

        orderState = getRuntimeContext().getState(descriptor);

        var metrics = getRuntimeContext().getMetricGroup();
        delayedOrdersDetected = metrics.counter("delayed_orders_detected");
        smsCommandsEmitted = metrics.counter("sms_commands_emitted");
        staleUpdatesIgnored = metrics.counter("stale_updates_ignored");
        invalidOrders = metrics.counter("invalid_messages");
    }

    @Override
    public void processElement(
            OrderState incoming,
            Context context,
            Collector<SmsCommand> out
    ) throws Exception {
        if (!isValid(incoming)) {
            DeadLetterEvent dlq = new DeadLetterEvent(
                    "orderId=" + incoming.getOrderId() + ", status=" + incoming.getStatus(),
                    "Invalid order state: missing required fields (orderId, status, expectedDeliveryTime, lastUpdatedAt)",
                    "Orders",
                    Instant.now()
            );
            context.output(INVALID_ORDER_TAG, dlq);
            invalidOrders.inc();
            return;
        }

        OrderDelayState current = orderState.value();

        if (current != null && isStaleUpdate(incoming, current)) {
            staleUpdatesIgnored.inc();
            return;
        }

        if (current == null) {
            current = OrderDelayState.fromOrderState(incoming);
        } else {
            current.updateFrom(incoming);
        }

        if (current.isTerminal()) {
            deleteRegisteredTimer(context, current);
            orderState.update(current);
            return;
        }

        if (current.isDelaySmsEmitted()) {
            orderState.update(current);
            return;
        }

        long now = context.timerService().currentProcessingTime();
        long expectedDeliveryTimeMs = current.getExpectedDeliveryTime().toEpochMilli();

        if (expectedDeliveryTimeMs <= now) {
            deleteRegisteredTimer(context, current);
            emitDelaySms(current, context.timerService().currentProcessingTime(), out);
            return;
        }

        registerOrUpdateTimer(context, current, expectedDeliveryTimeMs);
        orderState.update(current);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<SmsCommand> out
    ) throws Exception {
        OrderDelayState current = orderState.value();

        if (current == null) {
            return;
        }

        if (current.isTerminal()) {
            return;
        }

        if (current.isDelaySmsEmitted()) {
            return;
        }

        if (current.getExpectedDeliveryTime() == null) {
            return;
        }

        long expectedDeliveryTimeMs = current.getExpectedDeliveryTime().toEpochMilli();

        if (timestamp >= expectedDeliveryTimeMs) {
            emitDelaySms(current, context.timerService().currentProcessingTime(), out);
        }
    }

    private void emitDelaySms(
            OrderDelayState current,
            long currentProcessingTimeMs,
            Collector<SmsCommand> out
    ) throws Exception {
        SmsCommand command = SmsCommand.delaySms(
                current,
                Instant.ofEpochMilli(currentProcessingTimeMs)
        );

        delayedOrdersDetected.inc();
        smsCommandsEmitted.inc();
        out.collect(command);

        current.setDelaySmsEmitted(true);
        current.setRegisteredTimerTime(null);
        orderState.update(current);
    }

    private void registerOrUpdateTimer(
            Context context,
            OrderDelayState current,
            long newTimerTime
    ) {
        Long existingTimerTime = current.getRegisteredTimerTime();

        if (existingTimerTime != null && existingTimerTime == newTimerTime) {
            return;
        }

        if (existingTimerTime != null) {
            context.timerService().deleteProcessingTimeTimer(existingTimerTime);
        }

        context.timerService().registerProcessingTimeTimer(newTimerTime);
        current.setRegisteredTimerTime(newTimerTime);
    }

    private void deleteRegisteredTimer(Context context, OrderDelayState current) {
        Long existingTimerTime = current.getRegisteredTimerTime();

        if (existingTimerTime != null) {
            context.timerService().deleteProcessingTimeTimer(existingTimerTime);
            current.setRegisteredTimerTime(null);
        }
    }

    private boolean isValid(OrderState incoming) {
        return incoming.getOrderId() != null
                && incoming.getStatus() != null
                && incoming.getExpectedDeliveryTime() != null
                && incoming.getLastUpdatedAt() != null;
    }

    private boolean isStaleUpdate(OrderState incoming, OrderDelayState current) {
        if (current.getLastUpdatedAt() == null) {
            return false;
        }

        return incoming.getLastUpdatedAt().isBefore(current.getLastUpdatedAt());
    }
}