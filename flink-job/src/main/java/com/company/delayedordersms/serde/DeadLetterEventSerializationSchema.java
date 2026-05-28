package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Serializes DeadLetterEvent to JSON for writing to the dead-letter-events Kafka topic.
 */
public class DeadLetterEventSerializationSchema implements KafkaRecordSerializationSchema<DeadLetterEvent> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String topic;

    public DeadLetterEventSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DeadLetterEvent element, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] value = OBJECT_MAPPER.writeValueAsBytes(element);
            return new ProducerRecord<>(topic, null, element.getOccurredAt().toEpochMilli(), null, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize DeadLetterEvent", e);
        }
    }
}