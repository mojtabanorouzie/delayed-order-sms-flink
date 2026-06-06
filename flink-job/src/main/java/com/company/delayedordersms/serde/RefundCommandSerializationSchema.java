package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.RefundCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class RefundCommandSerializationSchema implements KafkaRecordSerializationSchema<RefundCommand> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String topic;

    public RefundCommandSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RefundCommand command,
            KafkaSinkContext context,
            Long timestamp
    ) {
        try {
            byte[] key = command.getCommandId().getBytes(StandardCharsets.UTF_8);
            byte[] value = OBJECT_MAPPER.writeValueAsBytes(command);
            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize refund command", e);
        }
    }
}
