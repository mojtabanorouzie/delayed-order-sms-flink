package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.DeadLetterEvent;
import com.company.delayedordersms.model.OrderState;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class OrderStateDeserializationFunction extends ProcessFunction<String, OrderState> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStateDeserializationFunction.class);

    public static final OutputTag<DeadLetterEvent> DEAD_LETTER_TAG =
            new OutputTag<DeadLetterEvent>("dead-letter") {};

    private transient OrderStateParser parser;
    private transient Counter parseErrors;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        this.parser = new OrderStateParser();

        var metrics = getRuntimeContext().getMetricGroup();
        parseErrors = metrics.counter("parse_errors");
    }

    @Override
    public void processElement(String value, Context ctx, Collector<OrderState> out) throws Exception {
        try {
            OrderState orderState = parser.parse(value);
            out.collect(orderState);
        } catch (Exception e) {
            LOG.warn("Failed to parse order state. payload={}", value, e);
            DeadLetterEvent dlq = new DeadLetterEvent(
                    value,
                    "JSON parse error: " + e.getMessage(),
                    "Orders",
                    Instant.now()
            );
            parseErrors.inc();
            ctx.output(DEAD_LETTER_TAG, dlq);
        }
    }
}