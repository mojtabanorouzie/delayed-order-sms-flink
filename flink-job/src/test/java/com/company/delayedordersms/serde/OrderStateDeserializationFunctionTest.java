package com.company.delayedordersms.serde;

import com.company.delayedordersms.model.OrderState;
import com.company.delayedordersms.model.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStateDeserializationFunctionTest {

    private final OrderStateParser parser = new OrderStateParser();

    @Test
    void shouldDeserializeValidJson() throws Exception {
        String json = """
                {
                    "orderId": "order-1",
                    "customerId": "cust-1",
                    "storeId": "store-1",
                    "status": "ACCEPTED",
                    "expectedDeliveryTime": "2026-06-01T12:00:00Z",
                    "createdAt": "2026-05-28T10:00:00Z",
                    "lastUpdatedAt": "2026-05-28T11:00:00Z",
                    "eventTime": "2026-05-28T11:00:00Z",
                    "stateLogs": [
                        {"status": "CREATED", "at": "2026-05-28T10:00:00Z"},
                        {"status": "ACCEPTED", "at": "2026-05-28T11:00:00Z"}
                    ],
                    "schemaVersion": 1
                }
                """;

        OrderState order = parser.parse(json);
        assertThat(order.getOrderId()).isEqualTo("order-1");
        assertThat(order.getCustomerId()).isEqualTo("cust-1");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.ACCEPTED);
        assertThat(order.getSchemaVersion()).isEqualTo(1);
    }

    @Test
    void shouldHandleInvalidJson() {
        String json = "not valid json at all {{{";

        // Invalid JSON should throw an exception from the parser
        assertThatThrownBy(() -> parser.parse(json))
                .isInstanceOf(Exception.class);
    }

    @Test
    void shouldHandleMissingRequiredFields() throws Exception {
        String json = """
                {
                    "orderId": "order-1",
                    "status": "ACCEPTED"
                }
                """;

        // Should not crash, even with missing fields.
        // Jackson's ObjectMapper doesn't enforce @JsonProperty(required) by default.
        // The process function's isValid() will catch missing fields downstream.
        OrderState order = parser.parse(json);
        assertThat(order).isNotNull();
        assertThat(order.getOrderId()).isEqualTo("order-1");
    }
}