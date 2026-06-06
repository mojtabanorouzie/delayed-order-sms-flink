package com.company.delayedordersms;

import com.company.delayedordersms.config.SurgePricingJobConfig;
import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.SurgePricingSignal;
import com.company.delayedordersms.model.WeatherData;
import com.company.delayedordersms.processor.DynamicSurgePricingCoProcessFunction;
import com.company.delayedordersms.serde.DeadLetterEventSerializationSchema;
import com.company.delayedordersms.serde.OrderStateDeserializationFunction;
import com.company.delayedordersms.serde.SurgePricingSignalSerializationSchema;
import com.company.delayedordersms.serde.WeatherDataDeserializationFunction;
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

public class SurgePricingJob {

    public static void main(String[] args) throws Exception {
        SurgePricingJobConfig config = SurgePricingJobConfig.fromArgs(args);

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

        // Source 1: Orders (partitioned by zoneId via keyBy)
        KafkaSource<String> ordersSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setTopics(config.ordersTopic())
                .setGroupId(config.consumerGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Source 2: Weather data (keyed by region = zoneId)
        // Uses latest() offset so live weather updates are picked up after job startup.
        // In production a compacted topic + earliest() would replay the full weather snapshot.
        KafkaSource<String> weatherSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setTopics(config.weatherTopic())
                .setGroupId(config.consumerGroupId() + "-weather")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<SurgePricingSignal> surgeSink = KafkaSink.<SurgePricingSignal>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setRecordSerializer(new SurgePricingSignalSerializationSchema(config.surgeSignalsTopic()))
                .build();

        KafkaSink<DeadLetterEvent> dlqSink = KafkaSink.<DeadLetterEvent>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setRecordSerializer(new DeadLetterEventSerializationSchema(config.deadLetterTopic()))
                .build();

        // Parse orders stream
        SingleOutputStreamOperator<OrderState> orders = env
                .fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "Kafka Source - Orders")
                .name("Read Orders")
                .process(new OrderStateDeserializationFunction())
                .name("Parse Order State");

        orders.getSideOutput(OrderStateDeserializationFunction.DEAD_LETTER_TAG)
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Order Parse Failures)");

        // Parse weather stream
        SingleOutputStreamOperator<WeatherData> weather = env
                .fromSource(weatherSource, WatermarkStrategy.noWatermarks(), "Kafka Source - Weather")
                .name("Read Weather")
                .process(new WeatherDataDeserializationFunction())
                .name("Parse Weather Data");

        weather.getSideOutput(WeatherDataDeserializationFunction.DEAD_LETTER_TAG)
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Weather Parse Failures)");

        // Connect both streams keyed by zone and compute surge multiplier
        SingleOutputStreamOperator<SurgePricingSignal> signals = orders
                .keyBy(OrderState::getZoneId)
                .connect(weather.keyBy(WeatherData::getRegion))
                .process(new DynamicSurgePricingCoProcessFunction(
                        config.stateTtlDays(),
                        config.windowSizeSeconds(),
                        config.atRiskThresholdMinutes(),
                        config.surgeThreshold(),
                        config.demandWeight(),
                        config.changeThreshold(),
                        config.maxMultiplier(),
                        config.surgeEnabled()
                ))
                .name("Compute Dynamic Surge Pricing");

        DataStream<DeadLetterEvent> invalidOrders = signals
                .getSideOutput(DynamicSurgePricingCoProcessFunction.INVALID_ORDER_TAG);

        invalidOrders
                .sinkTo(dlqSink)
                .name("Kafka Sink - Dead Letter (Invalid Orders)");

        if (config.surgeEnabled()) {
            signals
                    .sinkTo(surgeSink)
                    .name("Kafka Sink - Surge Pricing Signals");
        }

        env.execute("Dynamic Delivery Fee Surge with Weather Integration");
    }
}
