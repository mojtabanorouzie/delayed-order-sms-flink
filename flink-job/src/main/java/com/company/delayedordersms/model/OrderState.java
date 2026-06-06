package com.company.delayedordersms.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderState implements Serializable {

    private String orderId;
    private String customerId;
    private String storeId;
    private String courierId;
    private String zoneId;
    private OrderStatus status;
    private Instant expectedDeliveryTime;
    private Instant createdAt;
    private Instant lastUpdatedAt;
    private Instant eventTime;
    private double deliveryFee;
    private List<StateLog> stateLogs;
    private int schemaVersion;

    public OrderState() {
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getStoreId() {
        return storeId;
    }

    public String getCourierId() {
        return courierId;
    }

    public String getZoneId() {
        return zoneId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getExpectedDeliveryTime() {
        return expectedDeliveryTime;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public double getDeliveryFee() {
        return deliveryFee;
    }

    public List<StateLog> getStateLogs() {
        return stateLogs;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public void setCourierId(String courierId) {
        this.courierId = courierId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public void setExpectedDeliveryTime(Instant expectedDeliveryTime) {
        this.expectedDeliveryTime = expectedDeliveryTime;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public void setLastUpdatedAt(Instant lastUpdatedAt) {
        this.lastUpdatedAt = lastUpdatedAt;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public void setDeliveryFee(double deliveryFee) {
        this.deliveryFee = deliveryFee;
    }

    public void setStateLogs(List<StateLog> stateLogs) {
        this.stateLogs = stateLogs;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StateLog implements Serializable {
        private String status;
        private Instant at;

        public StateLog() {
        }

        public String getStatus() {
            return status;
        }

        public Instant getAt() {
            return at;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public void setAt(Instant at) {
            this.at = at;
        }
    }
}