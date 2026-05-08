package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.SmsCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class SmsCommandSerializationSchema implements KafkaRecordSerializationSchema<SmsCommand> {

    private final String topic;
    private transient ObjectMapper objectMapper;

    public SmsCommandSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            SmsCommand command,
            KafkaSinkContext context,
            Long timestamp
    ) {
        try {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            }

            byte[] key = command.getCommandId().getBytes(StandardCharsets.UTF_8);
            byte[] value = objectMapper.writeValueAsBytes(command);

            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize SMS command", e);
        }
    }
}