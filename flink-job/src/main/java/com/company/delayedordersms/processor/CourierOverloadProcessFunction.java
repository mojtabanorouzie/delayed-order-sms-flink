package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.CourierCommand;
import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;

/**
 * Emits PAUSE when a courier's active order count reaches overloadThreshold,
 * and RESUME when it falls below resumeThreshold.
 *
 * State: MapState<orderId, Boolean> tracks which orders are currently active for
 * the keyed courier. A separate Boolean ValueState records whether the courier is
 * currently paused to prevent duplicate commands.
 */
public class CourierOverloadProcessFunction extends KeyedProcessFunction<String, OrderState, CourierCommand> {

    public static final OutputTag<DeadLetterEvent> INVALID_ORDER_TAG =
            new OutputTag<DeadLetterEvent>("invalid-order-courier") {};

    private final int stateTtlDays;
    private final int overloadThreshold;
    private final int resumeThreshold;
    private final boolean pauseEnabled;

    private transient MapState<String, Boolean> activeOrders;
    private transient ValueState<Boolean> isPaused;

    private transient Counter pauseCommandsEmitted;
    private transient Counter resumeCommandsEmitted;
    private transient Counter invalidOrders;

    public CourierOverloadProcessFunction(
            int stateTtlDays, int overloadThreshold, int resumeThreshold, boolean pauseEnabled) {
        this.stateTtlDays = stateTtlDays;
        this.overloadThreshold = overloadThreshold;
        this.resumeThreshold = resumeThreshold;
        this.pauseEnabled = pauseEnabled;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofDays(stateTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        MapStateDescriptor<String, Boolean> activeOrdersDescriptor =
                new MapStateDescriptor<>("courier-active-orders", String.class, Boolean.class);
        activeOrdersDescriptor.enableTimeToLive(ttlConfig);
        activeOrders = getRuntimeContext().getMapState(activeOrdersDescriptor);

        ValueStateDescriptor<Boolean> isPausedDescriptor =
                new ValueStateDescriptor<>("courier-is-paused", Boolean.class);
        isPausedDescriptor.enableTimeToLive(ttlConfig);
        isPaused = getRuntimeContext().getState(isPausedDescriptor);

        var metrics = getRuntimeContext().getMetricGroup();
        pauseCommandsEmitted = metrics.counter("courier_pause_commands_emitted");
        resumeCommandsEmitted = metrics.counter("courier_resume_commands_emitted");
        invalidOrders = metrics.counter("courier_invalid_messages");
    }

    @Override
    public void processElement(
            OrderState incoming,
            Context ctx,
            Collector<CourierCommand> out
    ) throws Exception {
        if (!isValid(incoming)) {
            DeadLetterEvent dlq = new DeadLetterEvent(
                    "orderId=" + incoming.getOrderId() + ", courierId=" + incoming.getCourierId()
                            + ", status=" + incoming.getStatus(),
                    "Invalid order state: missing required fields (orderId, status, courierId)",
                    "Orders",
                    Instant.now()
            );
            ctx.output(INVALID_ORDER_TAG, dlq);
            invalidOrders.inc();
            return;
        }

        if (incoming.getStatus().isTerminal()) {
            activeOrders.remove(incoming.getOrderId());
        } else {
            activeOrders.put(incoming.getOrderId(), true);
        }

        int activeCount = countActiveOrders();
        boolean currentlyPaused = Boolean.TRUE.equals(isPaused.value());
        String courierId = ctx.getCurrentKey();

        if (activeCount >= overloadThreshold && !currentlyPaused) {
            isPaused.update(true);
            if (pauseEnabled) {
                out.collect(CourierCommand.pause(courierId, activeCount, Instant.now()));
                pauseCommandsEmitted.inc();
            }
        } else if (activeCount < resumeThreshold && currentlyPaused) {
            isPaused.update(false);
            if (pauseEnabled) {
                out.collect(CourierCommand.resume(courierId, activeCount, Instant.now()));
                resumeCommandsEmitted.inc();
            }
        }
    }

    private int countActiveOrders() throws Exception {
        int count = 0;
        var iter = activeOrders.values().iterator();
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        return count;
    }

    private boolean isValid(OrderState incoming) {
        return incoming.getOrderId() != null
                && incoming.getStatus() != null
                && incoming.getCourierId() != null
                && !incoming.getCourierId().isBlank();
    }
}
