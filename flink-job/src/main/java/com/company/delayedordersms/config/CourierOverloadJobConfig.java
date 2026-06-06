package com.company.delayedordersms.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public record CourierOverloadJobConfig(
        String kafkaBootstrapServers,
        String ordersTopic,
        String courierCommandsTopic,
        String deadLetterTopic,
        String consumerGroupId,
        String checkpointStoragePath,
        long checkpointIntervalMs,
        int parallelism,
        int restartAttempts,
        long restartDelayMs,
        int stateTtlDays,
        int overloadThreshold,
        int resumeThreshold,
        boolean pauseEnabled
) implements Serializable {

    public static CourierOverloadJobConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        return new CourierOverloadJobConfig(
                get(params, "kafka.bootstrap.servers", "localhost:9092"),
                get(params, "orders.topic", "Orders"),
                get(params, "courier.commands.topic", "courier-pause-commands"),
                get(params, "dead.letter.topic", "dead-letter-events"),
                get(params, "consumer.group.id", "courier-overload-flink"),
                get(params, "checkpoint.storage.path", "file:///tmp/flink-checkpoints-courier"),
                Long.parseLong(get(params, "checkpoint.interval.ms", "10000")),
                Integer.parseInt(get(params, "parallelism", "1")),
                Integer.parseInt(get(params, "restart.attempts", "3")),
                Long.parseLong(get(params, "restart.delay.ms", "5000")),
                Integer.parseInt(get(params, "state.ttl.days", "7")),
                Integer.parseInt(get(params, "overload.threshold", "8")),
                Integer.parseInt(get(params, "resume.threshold", "5")),
                Boolean.parseBoolean(get(params, "pause.enabled", "true"))
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
