package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.WeatherData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class WeatherDataDeserializationFunction extends ProcessFunction<String, WeatherData> {

    private static final Logger LOG = LoggerFactory.getLogger(WeatherDataDeserializationFunction.class);

    public static final OutputTag<DeadLetterEvent> DEAD_LETTER_TAG =
            new OutputTag<DeadLetterEvent>("invalid-weather-data") {};

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<WeatherData> out) throws Exception {
        try {
            WeatherData weather = mapper.readValue(value, WeatherData.class);
            if (weather.getRegion() == null || weather.getRegion().isBlank()) {
                ctx.output(DEAD_LETTER_TAG, new DeadLetterEvent(
                        value, "WeatherData missing region", "weather-data", Instant.now()));
                return;
            }
            out.collect(weather);
        } catch (Exception e) {
            LOG.warn("Failed to parse WeatherData: {}", value, e);
            ctx.output(DEAD_LETTER_TAG, new DeadLetterEvent(
                    value, "Parse error: " + e.getMessage(), "weather-data", Instant.now()));
        }
    }
}
