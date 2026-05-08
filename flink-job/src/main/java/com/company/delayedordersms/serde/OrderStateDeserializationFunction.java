package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.OrderState;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderStateDeserializationFunction extends RichFlatMapFunction<String, OrderState> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStateDeserializationFunction.class);

    private transient OrderStateParser parser;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.parser = new OrderStateParser();
    }

    @Override
    public void flatMap(String value, Collector<OrderState> out) {
        try {
            OrderState orderState = parser.parse(value);
            out.collect(orderState);
        } catch (Exception e) {
            LOG.warn("Failed to parse order state. payload={}", value, e);
        }
    }
}