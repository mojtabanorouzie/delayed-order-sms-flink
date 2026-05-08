package com.company.delayedordersms;

import com.company.delayedordersms.config.JobConfig;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.SmsCommand;
import com.company.delayedordersms.processor.DelayedOrderProcessFunction;
import com.company.delayedordersms.serde.OrderStateDeserializationFunction;
import com.company.delayedordersms.serde.SmsCommandSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DelayedOrderSmsJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(config.parallelism());

        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
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

        KafkaSink<SmsCommand> sink = KafkaSink.<SmsCommand>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setRecordSerializer(new SmsCommandSerializationSchema(config.smsCommandsTopic()))
                .build();

        SingleOutputStreamOperator<OrderState> orders = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source - Orders")
                .name("Read Orders")
                .flatMap(new OrderStateDeserializationFunction())
                .name("Parse Order State");

        orders
                .keyBy(OrderState::getOrderId)
                .process(new DelayedOrderProcessFunction())
                .name("Detect Delayed Orders")
                .sinkTo(sink)
                .name("Kafka Sink - SMS Commands");

        env.execute("Delayed Order SMS Detection Job");
    }
}