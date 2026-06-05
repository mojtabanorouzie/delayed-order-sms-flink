package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.OrderState;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.Serializable;

public class OrderStateParser implements Serializable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public OrderState parse(String json) throws Exception {
        return OBJECT_MAPPER.readValue(json, OrderState.class);
    }
}