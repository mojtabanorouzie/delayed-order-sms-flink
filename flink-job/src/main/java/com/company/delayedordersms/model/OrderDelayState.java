package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class OrderDelayState implements Serializable {

    private String orderId;
    private String customerId;
    private String storeId;
    private OrderStatus currentStatus;
    private Instant expectedDeliveryTime;
    private Instant lastUpdatedAt;
    private boolean delaySmsEmitted;
    private Long registeredTimerTime;

    public OrderDelayState() {
    }

    public static OrderDelayState fromOrderState(OrderState orderState) {
        OrderDelayState state = new OrderDelayState();
        state.orderId = orderState.getOrderId();
        state.customerId = orderState.getCustomerId();
        state.storeId = orderState.getStoreId();
        state.currentStatus = orderState.getStatus();
        state.expectedDeliveryTime = orderState.getExpectedDeliveryTime();
        state.lastUpdatedAt = orderState.getLastUpdatedAt();
        state.delaySmsEmitted = false;
        state.registeredTimerTime = null;
        return state;
    }

    public void updateFrom(OrderState orderState) {
        this.orderId = orderState.getOrderId();
        this.customerId = orderState.getCustomerId();
        this.storeId = orderState.getStoreId();
        this.currentStatus = orderState.getStatus();
        this.expectedDeliveryTime = orderState.getExpectedDeliveryTime();
        this.lastUpdatedAt = orderState.getLastUpdatedAt();
    }

    public boolean isTerminal() {
        return currentStatus != null && currentStatus.isTerminal();
    }

    public boolean isActive() {
        return currentStatus != null && currentStatus.isActive();
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

    public OrderStatus getCurrentStatus() {
        return currentStatus;
    }

    public Instant getExpectedDeliveryTime() {
        return expectedDeliveryTime;
    }

    public Instant getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public boolean isDelaySmsEmitted() {
        return delaySmsEmitted;
    }

    public Long getRegisteredTimerTime() {
        return registeredTimerTime;
    }

    public void setDelaySmsEmitted(boolean delaySmsEmitted) {
        this.delaySmsEmitted = delaySmsEmitted;
    }

    public void setRegisteredTimerTime(Long registeredTimerTime) {
        this.registeredTimerTime = registeredTimerTime;
    }
}