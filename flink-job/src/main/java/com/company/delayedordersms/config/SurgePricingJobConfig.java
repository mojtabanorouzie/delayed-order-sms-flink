package com.company.delayedordersms.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public record SurgePricingJobConfig(
        String kafkaBootstrapServers,
        String ordersTopic,
        String weatherTopic,
        String surgeSignalsTopic,
        String deadLetterTopic,
        String consumerGroupId,
        String checkpointStoragePath,
        long checkpointIntervalMs,
        int parallelism,
        int restartAttempts,
        long restartDelayMs,
        int stateTtlDays,
        int windowSizeSeconds,
        int atRiskThresholdMinutes,
        double surgeThreshold,
        double demandWeight,
        double changeThreshold,
        double maxMultiplier,
        boolean surgeEnabled
) implements Serializable {

    public static SurgePricingJobConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        return new SurgePricingJobConfig(
                get(params, "kafka.bootstrap.servers", "localhost:9092"),
                get(params, "orders.topic", "Orders"),
                get(params, "weather.topic", "weather-data"),
                get(params, "surge.signals.topic", "surge-pricing-signals"),
                get(params, "dead.letter.topic", "dead-letter-events"),
                get(params, "consumer.group.id", "surge-pricing-flink"),
                get(params, "checkpoint.storage.path", "file:///tmp/flink-checkpoints-surge"),
                Long.parseLong(get(params, "checkpoint.interval.ms", "10000")),
                Integer.parseInt(get(params, "parallelism", "1")),
                Integer.parseInt(get(params, "restart.attempts", "3")),
                Long.parseLong(get(params, "restart.delay.ms", "5000")),
                Integer.parseInt(get(params, "state.ttl.days", "7")),
                Integer.parseInt(get(params, "window.size.seconds", "60")),
                Integer.parseInt(get(params, "at.risk.threshold.minutes", "25")),
                Double.parseDouble(get(params, "surge.threshold", "1.15")),
                Double.parseDouble(get(params, "demand.weight", "0.5")),
                Double.parseDouble(get(params, "change.threshold", "0.05")),
                Double.parseDouble(get(params, "max.multiplier", "3.0")),
                Boolean.parseBoolean(get(params, "surge.enabled", "true"))
        );
    }

    private static String get(ParameterTool params, String key, String defaultValue) {
        String envKey = key.toUpperCase().replace(".", "_");
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        return params.get(key, defaultValue);
    }
}
