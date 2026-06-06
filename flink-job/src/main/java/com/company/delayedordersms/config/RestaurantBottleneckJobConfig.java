package com.company.delayedordersms.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public record RestaurantBottleneckJobConfig(
        String kafkaBootstrapServers,
        String ordersTopic,
        String restaurantAlertsTopic,
        String deadLetterTopic,
        String consumerGroupId,
        String checkpointStoragePath,
        long checkpointIntervalMs,
        int parallelism,
        int restartAttempts,
        long restartDelayMs,
        int stateTtlDays,
        int windowSizeSeconds,
        int alertThresholdMinutes,
        int baselineMinutes,
        boolean alertEnabled
) implements Serializable {

    public static RestaurantBottleneckJobConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        return new RestaurantBottleneckJobConfig(
                get(params, "kafka.bootstrap.servers", "localhost:9092"),
                get(params, "orders.topic", "Orders"),
                get(params, "restaurant.alerts.topic", "restaurant-alerts"),
                get(params, "dead.letter.topic", "dead-letter-events"),
                get(params, "consumer.group.id", "restaurant-bottleneck-flink"),
                get(params, "checkpoint.storage.path", "file:///tmp/flink-checkpoints-restaurant"),
                Long.parseLong(get(params, "checkpoint.interval.ms", "10000")),
                Integer.parseInt(get(params, "parallelism", "1")),
                Integer.parseInt(get(params, "restart.attempts", "3")),
                Long.parseLong(get(params, "restart.delay.ms", "5000")),
                Integer.parseInt(get(params, "state.ttl.days", "7")),
                Integer.parseInt(get(params, "window.size.seconds", "300")),
                Integer.parseInt(get(params, "alert.threshold.minutes", "15")),
                Integer.parseInt(get(params, "baseline.minutes", "8")),
                Boolean.parseBoolean(get(params, "alert.enabled", "true"))
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
