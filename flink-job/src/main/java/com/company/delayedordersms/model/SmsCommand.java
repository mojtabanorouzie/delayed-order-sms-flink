package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class SmsCommand implements Serializable {

    private String commandId;
    private String commandType;
    private String orderId;
    private String customerId;
    private String storeId;
    private String reason;
    private Instant expectedDeliveryTime;
    private Instant createdAt;
    private int schemaVersion;

    public SmsCommand() {
    }

    public static SmsCommand delaySms(OrderDelayState state, Instant createdAt) {
        SmsCommand command = new SmsCommand();
        command.commandId = state.getOrderId() + ":DELAY_SMS";
        command.commandType = "SEND_DELAY_SMS";
        command.orderId = state.getOrderId();
        command.customerId = state.getCustomerId();
        command.storeId = state.getStoreId();
        command.reason = "ORDER_DELAYED";
        command.expectedDeliveryTime = state.getExpectedDeliveryTime();
        command.createdAt = createdAt;
        command.schemaVersion = 1;
        return command;
    }

    public String getCommandId() {
        return commandId;
    }

    public String getCommandType() {
        return commandType;
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

    public String getReason() {
        return reason;
    }

    public Instant getExpectedDeliveryTime() {
        return expectedDeliveryTime;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setCommandId(String commandId) {
        this.commandId = commandId;
    }

    public void setCommandType(String commandType) {
        this.commandType = commandType;
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

    public void setReason(String reason) {
        this.reason = reason;
    }

    public void setExpectedDeliveryTime(Instant expectedDeliveryTime) {
        this.expectedDeliveryTime = expectedDeliveryTime;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }
}