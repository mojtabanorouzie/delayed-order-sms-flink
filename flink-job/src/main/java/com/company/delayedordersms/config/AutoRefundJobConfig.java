package com.company.delayedordersms.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public record AutoRefundJobConfig(
        String kafkaBootstrapServers,
        String ordersTopic,
        String refundCommandsTopic,
        String deadLetterTopic,
        String consumerGroupId,
        String checkpointStoragePath,
        long checkpointIntervalMs,
        int parallelism,
        int restartAttempts,
        long restartDelayMs,
        int stateTtlDays,
        int refundDelayMinutes,
        boolean refundEnabled
) implements Serializable {

    public static AutoRefundJobConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        return new AutoRefundJobConfig(
                get(params, "kafka.bootstrap.servers", "localhost:9092"),
                get(params, "orders.topic", "Orders"),
                get(params, "refund.commands.topic", "refund-commands"),
                get(params, "dead.letter.topic", "dead-letter-events"),
                get(params, "consumer.group.id", "auto-refund-flink"),
                get(params, "checkpoint.storage.path", "file:///tmp/flink-checkpoints-refund"),
                Long.parseLong(get(params, "checkpoint.interval.ms", "10000")),
                Integer.parseInt(get(params, "parallelism", "1")),
                Integer.parseInt(get(params, "restart.attempts", "3")),
                Long.parseLong(get(params, "restart.delay.ms", "5000")),
                Integer.parseInt(get(params, "state.ttl.days", "7")),
                Integer.parseInt(get(params, "refund.delay.minutes", "30")),
                Boolean.parseBoolean(get(params, "refund.enabled", "true"))
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
