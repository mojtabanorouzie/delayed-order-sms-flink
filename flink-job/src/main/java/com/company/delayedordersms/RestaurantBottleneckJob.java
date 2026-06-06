package com.company.delayedordersms;

import com.company.delayedordersms.config.RestaurantBottleneckJobConfig;
import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OpsAlert;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.processor.RestaurantBottleneckProcessFunction;
import com.company.delayedordersms.serde.DeadLetterEventSerializationSchema;
import com.company.delayedordersms.serde.OpsAlertSerializationSchema;
import com.company.delayedordersms.serde.OrderStateDeserializationFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RestaurantBottleneckJob {

    public static void main(String[] args) throws Exception {
        RestaurantBottleneckJobConfig config = RestaurantBottleneckJobConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(config.parallelism());

        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(config.checkpointStoragePath());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        config.restartAttempts(),
                        config.restartDelayMs()
                )
        );

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setTopics(config.ordersTopic())
                .setGroupId(config.consumerGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<OpsAlert> alertSink = KafkaSink.<OpsAlert>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setRecordSerializer(new OpsAlertSerializationSchema(config.restaurantAlertsTopic()))
                .build();

        KafkaSink<DeadLetterEvent> dlqSink = KafkaSink.<DeadLetterEvent>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setRecordSerializer(new DeadLetterEventSerializationSchema(config.deadLetterTopic()))
                .build();

        SingleOutputStreamOperator<OrderState> orders = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source - Orders")
                .name("Read Orders")
                .process(new OrderStateDeserializationFunction())
                .name("Parse Order State");

        orders.getSideOutput(OrderStateDeserializationFunction.DEAD_LETTER_TAG)
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Parse Failures)");

        SingleOutputStreamOperator<OpsAlert> alerts = orders
                .keyBy(OrderState::getStoreId)
                .process(new RestaurantBottleneckProcessFunction(
                        config.stateTtlDays(),
                        config.windowSizeSeconds(),
                        config.alertThresholdMinutes(),
                        config.baselineMinutes(),
                        config.alertEnabled()
                ))
                .name("Detect Restaurant Queue Bottleneck");

        DataStream<DeadLetterEvent> invalidOrders = alerts
                .getSideOutput(RestaurantBottleneckProcessFunction.INVALID_ORDER_TAG);

        invalidOrders
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Invalid Orders)");

        if (config.alertEnabled()) {
            alerts
                    .sinkTo(alertSink)
                    .name("Kafka Sink - Restaurant Alerts");
        }

        env.execute("Restaurant Queue Bottleneck Detection");
    }
}
