package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class RefundCommand implements Serializable {

    private String commandId;
    private String commandType;
    private String orderId;
    private String customerId;
    private String storeId;
    private double refundAmount;
    private String reason;
    private Instant expectedDeliveryTime;
    private Instant detectedAt;
    private int schemaVersion;

    public RefundCommand() {
    }

    public static RefundCommand forDelayBreach(RefundState state, Instant detectedAt, int delayMinutes) {
        RefundCommand cmd = new RefundCommand();
        cmd.commandId = state.getOrderId() + ":REFUND_" + delayMinutes + "MIN";
        cmd.commandType = "AUTO_REFUND";
        cmd.orderId = state.getOrderId();
        cmd.customerId = state.getCustomerId();
        cmd.storeId = state.getStoreId();
        cmd.refundAmount = state.getDeliveryFee() * 0.50;
        cmd.reason = "DELAY_" + delayMinutes + "_MIN_BREACH";
        cmd.expectedDeliveryTime = state.getExpectedDeliveryTime();
        cmd.detectedAt = detectedAt;
        cmd.schemaVersion = 1;
        return cmd;
    }

    public String getCommandId() { return commandId; }
    public String getCommandType() { return commandType; }
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public String getStoreId() { return storeId; }
    public double getRefundAmount() { return refundAmount; }
    public String getReason() { return reason; }
    public Instant getExpectedDeliveryTime() { return expectedDeliveryTime; }
    public Instant getDetectedAt() { return detectedAt; }
    public int getSchemaVersion() { return schemaVersion; }

    public void setCommandId(String commandId) { this.commandId = commandId; }
    public void setCommandType(String commandType) { this.commandType = commandType; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public void setStoreId(String storeId) { this.storeId = storeId; }
    public void setRefundAmount(double refundAmount) { this.refundAmount = refundAmount; }
    public void setReason(String reason) { this.reason = reason; }
    public void setExpectedDeliveryTime(Instant expectedDeliveryTime) { this.expectedDeliveryTime = expectedDeliveryTime; }
    public void setDetectedAt(Instant detectedAt) { this.detectedAt = detectedAt; }
    public void setSchemaVersion(int schemaVersion) { this.schemaVersion = schemaVersion; }
}
