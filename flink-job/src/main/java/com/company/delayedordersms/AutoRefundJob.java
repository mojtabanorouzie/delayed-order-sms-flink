package com.company.delayedordersms;

import com.company.delayedordersms.config.AutoRefundJobConfig;
import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.RefundCommand;
import com.company.delayedordersms.processor.DelayedOrderRefundProcessFunction;
import com.company.delayedordersms.serde.DeadLetterEventSerializationSchema;
import com.company.delayedordersms.serde.OrderStateDeserializationFunction;
import com.company.delayedordersms.serde.RefundCommandSerializationSchema;
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

public class AutoRefundJob {

    public static void main(String[] args) throws Exception {
        AutoRefundJobConfig config = AutoRefundJobConfig.fromArgs(args);

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

        KafkaSink<RefundCommand> refundSink = KafkaSink.<RefundCommand>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setRecordSerializer(new RefundCommandSerializationSchema(config.refundCommandsTopic()))
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

        DataStream<DeadLetterEvent> parseFailures = orders
                .getSideOutput(OrderStateDeserializationFunction.DEAD_LETTER_TAG);

        parseFailures
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Parse Failures)");

        SingleOutputStreamOperator<RefundCommand> refundCommands = orders
                .keyBy(OrderState::getOrderId)
                .process(new DelayedOrderRefundProcessFunction(
                        config.stateTtlDays(),
                        config.refundDelayMinutes()
                ))
                .name("Detect Refund-Eligible Orders");

        DataStream<DeadLetterEvent> invalidOrders = refundCommands
                .getSideOutput(DelayedOrderRefundProcessFunction.INVALID_ORDER_TAG);

        invalidOrders
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Invalid Orders)");

        // Only emit to refund-commands if refund feature is enabled
        if (config.refundEnabled()) {
            refundCommands
                    .sinkTo(refundSink)
                    .name("Kafka Sink - Refund Commands");
        }

        env.execute("Auto-Refund for Severely Delayed Orders");
    }
}
