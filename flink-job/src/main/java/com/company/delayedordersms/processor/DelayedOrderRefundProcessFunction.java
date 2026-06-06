package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.RefundCommand;
import com.company.delayedordersms.model.RefundState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import java.time.Duration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

/**
 * Emits a RefundCommand when an order breaches its ETA by refundDelayMinutes.
 *
 * Timer strategy: processing-time timer at expectedDeliveryTime + refundDelayMs.
 * Idempotency: refundEmitted flag prevents duplicate commands per order.
 * State TTL: 7 days, cleans up stuck orders that never reach a terminal state.
 */
public class DelayedOrderRefundProcessFunction extends KeyedProcessFunction<String, OrderState, RefundCommand> {

    public static final OutputTag<DeadLetterEvent> INVALID_ORDER_TAG =
            new OutputTag<DeadLetterEvent>("invalid-order-refund") {};

    private final int stateTtlDays;
    private final long refundDelayMs;
    private final int refundDelayMinutes;

    private transient ValueState<RefundState> refundState;

    private transient Counter refundsEmitted;
    private transient Counter staleUpdatesIgnored;
    private transient Counter invalidOrders;
    private transient Counter terminalOrdersSkipped;

    public DelayedOrderRefundProcessFunction(int stateTtlDays, int refundDelayMinutes) {
        this.stateTtlDays = stateTtlDays;
        this.refundDelayMinutes = refundDelayMinutes;
        this.refundDelayMs = (long) refundDelayMinutes * 60 * 1000;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        ValueStateDescriptor<RefundState> descriptor =
                new ValueStateDescriptor<>("refund-state", RefundState.class);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofDays(stateTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
        descriptor.enableTimeToLive(ttlConfig);

        refundState = getRuntimeContext().getState(descriptor);

        var metrics = getRuntimeContext().getMetricGroup();
        refundsEmitted = metrics.counter("refund_commands_emitted");
        staleUpdatesIgnored = metrics.counter("refund_stale_updates_ignored");
        invalidOrders = metrics.counter("refund_invalid_messages");
        terminalOrdersSkipped = metrics.counter("refund_terminal_orders_skipped");
    }

    @Override
    public void processElement(
            OrderState incoming,
            Context context,
            Collector<RefundCommand> out
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

        RefundState current = refundState.value();

        if (current != null && isStaleUpdate(incoming, current)) {
            staleUpdatesIgnored.inc();
            return;
        }

        if (current == null) {
            current = RefundState.fromOrderState(incoming);
        } else {
            current.updateFrom(incoming);
        }

        if (current.isTerminal()) {
            deleteRegisteredTimer(context, current);
            refundState.update(current);
            terminalOrdersSkipped.inc();
            return;
        }

        if (current.isRefundEmitted()) {
            refundState.update(current);
            return;
        }

        long now = context.timerService().currentProcessingTime();
        long refundTriggerTime = current.getExpectedDeliveryTime().toEpochMilli() + refundDelayMs;

        if (refundTriggerTime <= now) {
            // ETA + delay has already passed — emit immediately
            deleteRegisteredTimer(context, current);
            emitRefund(current, now, out);
            return;
        }

        registerOrUpdateTimer(context, current, refundTriggerTime);
        refundState.update(current);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<RefundCommand> out
    ) throws Exception {
        RefundState current = refundState.value();

        if (current == null || current.isTerminal() || current.isRefundEmitted()) {
            return;
        }

        if (current.getExpectedDeliveryTime() == null) {
            return;
        }

        long refundTriggerTime = current.getExpectedDeliveryTime().toEpochMilli() + refundDelayMs;

        if (timestamp >= refundTriggerTime) {
            emitRefund(current, context.timerService().currentProcessingTime(), out);
        }
    }

    private void emitRefund(
            RefundState current,
            long currentProcessingTimeMs,
            Collector<RefundCommand> out
    ) throws Exception {
        RefundCommand command = RefundCommand.forDelayBreach(
                current,
                Instant.ofEpochMilli(currentProcessingTimeMs),
                refundDelayMinutes
        );

        out.collect(command);
        refundsEmitted.inc();

        current.setRefundEmitted(true);
        current.setRegisteredTimerTime(null);
        refundState.update(current);
    }

    private void registerOrUpdateTimer(Context context, RefundState current, long newTimerTime) {
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

    private void deleteRegisteredTimer(Context context, RefundState current) {
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

    private boolean isStaleUpdate(OrderState incoming, RefundState current) {
        if (current.getLastUpdatedAt() == null) {
            return false;
        }

        return incoming.getLastUpdatedAt().isBefore(current.getLastUpdatedAt());
    }
}
