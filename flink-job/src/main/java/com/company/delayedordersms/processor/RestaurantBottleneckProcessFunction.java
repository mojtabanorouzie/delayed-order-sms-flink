package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OpsAlert;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderStatus;
import com.company.delayedordersms.model.RestaurantWindowState;
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
 * Detects restaurant queue bottlenecks by measuring per-order pickup time (ACCEPTED → PICKED_UP).
 *
 * A processing-time tumbling window (default 5 min) accumulates pickup-time deltas per store.
 * When the window fires, if the average exceeds alertThresholdMinutes a CRITICAL OpsAlert is emitted;
 * if it exceeds baselineMinutes a WARNING is emitted. No alert is emitted when the avg is healthy.
 *
 * State TTL prevents stale orders (accepted but never picked up) from accumulating indefinitely.
 */
public class RestaurantBottleneckProcessFunction extends KeyedProcessFunction<String, OrderState, OpsAlert> {

    public static final OutputTag<DeadLetterEvent> INVALID_ORDER_TAG =
            new OutputTag<DeadLetterEvent>("invalid-order-restaurant") {};

    private final int stateTtlDays;
    private final long windowSizeMs;
    private final long alertThresholdMs;
    private final long baselineMs;
    private final boolean alertEnabled;

    private transient MapState<String, Long> acceptedOrders;
    private transient ValueState<RestaurantWindowState> currentWindow;

    private transient Counter alertsEmitted;
    private transient Counter ordersTracked;
    private transient Counter invalidOrders;

    public RestaurantBottleneckProcessFunction(
            int stateTtlDays,
            int windowSizeSeconds,
            int alertThresholdMinutes,
            int baselineMinutes,
            boolean alertEnabled
    ) {
        this.stateTtlDays = stateTtlDays;
        this.windowSizeMs = (long) windowSizeSeconds * 1000;
        this.alertThresholdMs = (long) alertThresholdMinutes * 60 * 1000;
        this.baselineMs = (long) baselineMinutes * 60 * 1000;
        this.alertEnabled = alertEnabled;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofDays(stateTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        MapStateDescriptor<String, Long> acceptedDescriptor =
                new MapStateDescriptor<>("restaurant-accepted-orders", String.class, Long.class);
        acceptedDescriptor.enableTimeToLive(ttlConfig);
        acceptedOrders = getRuntimeContext().getMapState(acceptedDescriptor);

        ValueStateDescriptor<RestaurantWindowState> windowDescriptor =
                new ValueStateDescriptor<>("restaurant-window-state", RestaurantWindowState.class);
        windowDescriptor.enableTimeToLive(ttlConfig);
        currentWindow = getRuntimeContext().getState(windowDescriptor);

        var metrics = getRuntimeContext().getMetricGroup();
        alertsEmitted = metrics.counter("restaurant_alerts_emitted");
        ordersTracked = metrics.counter("restaurant_orders_tracked");
        invalidOrders = metrics.counter("restaurant_invalid_messages");
    }

    @Override
    public void processElement(
            OrderState incoming,
            Context ctx,
            Collector<OpsAlert> out
    ) throws Exception {
        if (!isValid(incoming)) {
            DeadLetterEvent dlq = new DeadLetterEvent(
                    "orderId=" + incoming.getOrderId() + ", storeId=" + incoming.getStoreId()
                            + ", status=" + incoming.getStatus(),
                    "Invalid order state: missing required fields (orderId, status, lastUpdatedAt)",
                    "Orders",
                    Instant.now()
            );
            ctx.output(INVALID_ORDER_TAG, dlq);
            invalidOrders.inc();
            return;
        }

        if (incoming.getStatus() == OrderStatus.ACCEPTED) {
            acceptedOrders.put(incoming.getOrderId(), incoming.getLastUpdatedAt().toEpochMilli());
            ordersTracked.inc();
            ensureWindowTimer(ctx);
        } else if (incoming.getStatus() == OrderStatus.PICKED_UP) {
            Long acceptedAtMs = acceptedOrders.get(incoming.getOrderId());
            if (acceptedAtMs != null) {
                long pickupDeltaMs = incoming.getLastUpdatedAt().toEpochMilli() - acceptedAtMs;
                if (pickupDeltaMs > 0) {
                    addToCurrentWindow(pickupDeltaMs, ctx);
                }
                acceptedOrders.remove(incoming.getOrderId());
            }
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<OpsAlert> out
    ) throws Exception {
        RestaurantWindowState window = currentWindow.value();
        if (window == null || window.getCompletedOrderCount() == 0) {
            currentWindow.clear();
            return;
        }

        double avgPickupMs = (double) window.getSumPickupMillis() / window.getCompletedOrderCount();
        double avgPickupMinutes = avgPickupMs / 60_000.0;
        String storeId = ctx.getCurrentKey();
        Instant windowEnd = Instant.ofEpochMilli(timestamp);
        Instant windowStart = Instant.ofEpochMilli(timestamp - windowSizeMs);

        String severity = null;
        if (avgPickupMs > alertThresholdMs) {
            severity = "CRITICAL";
        } else if (avgPickupMs > baselineMs) {
            severity = "WARNING";
        }

        if (severity != null && alertEnabled) {
            OpsAlert alert = OpsAlert.bottleneck(
                    storeId, avgPickupMinutes, window.getCompletedOrderCount(),
                    windowStart, windowEnd, severity, Instant.now());
            out.collect(alert);
            alertsEmitted.inc();
        }

        currentWindow.clear();
    }

    private void ensureWindowTimer(Context ctx) throws Exception {
        RestaurantWindowState window = currentWindow.value();
        if (window == null) {
            long now = ctx.timerService().currentProcessingTime();
            long windowEnd = now + windowSizeMs;
            currentWindow.update(new RestaurantWindowState(windowEnd, 0, 0L));
            ctx.timerService().registerProcessingTimeTimer(windowEnd);
        }
    }

    private void addToCurrentWindow(long pickupDeltaMs, Context ctx) throws Exception {
        RestaurantWindowState window = currentWindow.value();
        if (window == null) {
            long now = ctx.timerService().currentProcessingTime();
            long windowEnd = now + windowSizeMs;
            window = new RestaurantWindowState(windowEnd, 0, 0L);
            ctx.timerService().registerProcessingTimeTimer(windowEnd);
        }
        window.setCompletedOrderCount(window.getCompletedOrderCount() + 1);
        window.setSumPickupMillis(window.getSumPickupMillis() + pickupDeltaMs);
        currentWindow.update(window);
    }

    private boolean isValid(OrderState incoming) {
        return incoming.getOrderId() != null
                && incoming.getStatus() != null
                && incoming.getLastUpdatedAt() != null;
    }
}
