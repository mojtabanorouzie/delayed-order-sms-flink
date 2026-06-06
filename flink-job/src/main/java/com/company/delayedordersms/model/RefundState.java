package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class RefundState implements Serializable {

    private String orderId;
    private String customerId;
    private String storeId;
    private OrderStatus currentStatus;
    private Instant expectedDeliveryTime;
    private Instant lastUpdatedAt;
    private double deliveryFee;
    private boolean refundEmitted;
    private Long registeredTimerTime;

    public RefundState() {
    }

    public static RefundState fromOrderState(OrderState orderState) {
        RefundState state = new RefundState();
        state.orderId = orderState.getOrderId();
        state.customerId = orderState.getCustomerId();
        state.storeId = orderState.getStoreId();
        state.currentStatus = orderState.getStatus();
        state.expectedDeliveryTime = orderState.getExpectedDeliveryTime();
        state.lastUpdatedAt = orderState.getLastUpdatedAt();
        state.deliveryFee = orderState.getDeliveryFee();
        state.refundEmitted = false;
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
        this.deliveryFee = orderState.getDeliveryFee();
    }

    public boolean isTerminal() {
        return currentStatus != null && currentStatus.isTerminal();
    }

    public boolean isActive() {
        return currentStatus != null && currentStatus.isActive();
    }

    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getStoreId() { return storeId; }
    public OrderStatus getCurrentStatus() { return currentStatus; }
    public Instant getExpectedDeliveryTime() { return expectedDeliveryTime; }
    public Instant getLastUpdatedAt() { return lastUpdatedAt; }
    public double getDeliveryFee() { return deliveryFee; }
    public boolean isRefundEmitted() { return refundEmitted; }
    public Long getRegisteredTimerTime() { return registeredTimerTime; }

    public void setRefundEmitted(boolean refundEmitted) { this.refundEmitted = refundEmitted; }
    public void setRegisteredTimerTime(Long registeredTimerTime) { this.registeredTimerTime = registeredTimerTime; }
}
