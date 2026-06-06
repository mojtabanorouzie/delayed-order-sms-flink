package com.company.delayedordersms.processor;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderStatus;
import com.company.delayedordersms.model.SurgePricingSignal;
import com.company.delayedordersms.model.SurgeWindowState;
import com.company.delayedordersms.model.WeatherCondition;
import com.company.delayedordersms.model.WeatherData;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;

/**
 * Combines per-zone order demand with live weather data to emit dynamic surge pricing signals.
 *
 * Stream 1 (orders, keyed by zoneId): CREATED orders increment the demand window; at-risk orders
 * (expectedDeliveryTime > atRiskThresholdMs from now) increment the at-risk counter.
 *
 * Stream 2 (weather, keyed by region=zoneId): cached in ValueState per zone with TTL.
 *
 * A processing-time tumbling window fires every windowSizeSeconds. On each fire, the combined
 * multiplier is computed as: 1.0 + (demandFactor × demandWeight) × weatherFactor.
 * A signal is emitted if combinedMultiplier >= surgeThreshold and the multiplier changed by more
 * than changeThreshold relative to the last emission (prevents chatty near-identical signals).
 */
public class DynamicSurgePricingCoProcessFunction
        extends KeyedCoProcessFunction<String, OrderState, WeatherData, SurgePricingSignal> {

    public static final OutputTag<DeadLetterEvent> INVALID_ORDER_TAG =
            new OutputTag<DeadLetterEvent>("invalid-order-surge") {};

    private final int stateTtlDays;
    private final long windowSizeMs;
    private final long atRiskThresholdMs;
    private final double surgeThreshold;
    private final double demandWeight;
    private final double changeThreshold;
    private final double maxMultiplier;
    private final boolean surgeEnabled;

    private transient ValueState<WeatherData> currentWeather;
    private transient ValueState<SurgeWindowState> demandWindow;
    private transient ValueState<Double> lastEmittedMultiplier;

    private transient Counter signalsEmitted;
    private transient Counter weatherUpdates;
    private transient Counter invalidOrders;

    public DynamicSurgePricingCoProcessFunction(
            int stateTtlDays,
            int windowSizeSeconds,
            int atRiskThresholdMinutes,
            double surgeThreshold,
            double demandWeight,
            double changeThreshold,
            double maxMultiplier,
            boolean surgeEnabled
    ) {
        this.stateTtlDays = stateTtlDays;
        this.windowSizeMs = (long) windowSizeSeconds * 1_000;
        this.atRiskThresholdMs = (long) atRiskThresholdMinutes * 60 * 1_000;
        this.surgeThreshold = surgeThreshold;
        this.demandWeight = demandWeight;
        this.changeThreshold = changeThreshold;
        this.maxMultiplier = maxMultiplier;
        this.surgeEnabled = surgeEnabled;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofDays(stateTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<WeatherData> weatherDescriptor =
                new ValueStateDescriptor<>("zone-weather", WeatherData.class);
        weatherDescriptor.enableTimeToLive(ttlConfig);
        currentWeather = getRuntimeContext().getState(weatherDescriptor);

        ValueStateDescriptor<SurgeWindowState> windowDescriptor =
                new ValueStateDescriptor<>("surge-demand-window", SurgeWindowState.class);
        windowDescriptor.enableTimeToLive(ttlConfig);
        demandWindow = getRuntimeContext().getState(windowDescriptor);

        ValueStateDescriptor<Double> multiplierDescriptor =
                new ValueStateDescriptor<>("last-emitted-multiplier", Types.DOUBLE);
        multiplierDescriptor.enableTimeToLive(ttlConfig);
        lastEmittedMultiplier = getRuntimeContext().getState(multiplierDescriptor);

        var metrics = getRuntimeContext().getMetricGroup();
        signalsEmitted = metrics.counter("surge_signals_emitted");
        weatherUpdates = metrics.counter("surge_weather_updates");
        invalidOrders = metrics.counter("surge_invalid_orders");
    }

    @Override
    public void processElement1(
            OrderState order,
            Context ctx,
            Collector<SurgePricingSignal> out
    ) throws Exception {
        if (!isValidOrder(order)) {
            ctx.output(INVALID_ORDER_TAG, new DeadLetterEvent(
                    "orderId=" + order.getOrderId() + ", zoneId=" + order.getZoneId()
                            + ", status=" + order.getStatus(),
                    "Invalid order: missing required fields (orderId, status)",
                    "Orders",
                    Instant.now()));
            invalidOrders.inc();
            return;
        }

        if (order.getStatus() == OrderStatus.CREATED) {
            long processingTime = ctx.timerService().currentProcessingTime();
            boolean atRisk = isAtRisk(order, processingTime);
            updateDemandWindow(atRisk, ctx);
        }
    }

    @Override
    public void processElement2(
            WeatherData weather,
            Context ctx,
            Collector<SurgePricingSignal> out
    ) throws Exception {
        currentWeather.update(weather);
        weatherUpdates.inc();
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<SurgePricingSignal> out
    ) throws Exception {
        SurgeWindowState window = demandWindow.value();
        if (window == null || window.getOrdersInWindow() == 0) {
            demandWindow.clear();
            return;
        }

        double demandFactor = (double) window.getAtRiskOrderCount() / window.getOrdersInWindow();
        WeatherData weather = currentWeather.value();
        double weatherFactor = weather != null
                ? getWeatherFactor(WeatherCondition.fromString(weather.getCondition()))
                : 1.0;
        double combined = Math.min(1.0 + (demandFactor * demandWeight) * weatherFactor, maxMultiplier);

        if (combined >= surgeThreshold && surgeEnabled) {
            Double last = lastEmittedMultiplier.value();
            boolean changed = last == null
                    || Math.abs(combined - last) / Math.max(last, 1.0) >= changeThreshold;
            if (changed) {
                String zoneId = ctx.getCurrentKey();
                String conditionStr = weather != null ? weather.getCondition() : "UNKNOWN";
                SurgePricingSignal signal = SurgePricingSignal.surge(
                        zoneId, timestamp, combined, demandFactor, weatherFactor,
                        conditionStr, window.getOrdersInWindow(), window.getAtRiskOrderCount(),
                        Instant.now());
                out.collect(signal);
                lastEmittedMultiplier.update(combined);
                signalsEmitted.inc();
            }
        }

        demandWindow.clear();
    }

    private void updateDemandWindow(boolean atRisk, Context ctx) throws Exception {
        SurgeWindowState window = demandWindow.value();
        if (window == null) {
            long now = ctx.timerService().currentProcessingTime();
            long windowEnd = now + windowSizeMs;
            window = new SurgeWindowState(windowEnd, 0, 0);
            ctx.timerService().registerProcessingTimeTimer(windowEnd);
        }
        window.setOrdersInWindow(window.getOrdersInWindow() + 1);
        if (atRisk) {
            window.setAtRiskOrderCount(window.getAtRiskOrderCount() + 1);
        }
        demandWindow.update(window);
    }

    private boolean isAtRisk(OrderState order, long processingTimeMs) {
        if (order.getExpectedDeliveryTime() == null) return false;
        long remaining = order.getExpectedDeliveryTime().toEpochMilli() - processingTimeMs;
        return remaining > atRiskThresholdMs;
    }

    private boolean isValidOrder(OrderState order) {
        return order.getOrderId() != null && order.getStatus() != null;
    }

    static double getWeatherFactor(WeatherCondition condition) {
        return switch (condition) {
            case CLEAR -> 0.95;
            case CLOUDY -> 1.0;
            case RAIN -> 1.2;
            case SNOW -> 1.4;
            default -> 1.0;
        };
    }
}
