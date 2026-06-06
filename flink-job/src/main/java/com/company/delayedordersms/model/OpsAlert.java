package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class OpsAlert implements Serializable {

    private String alertId;
    private String alertType;
    private String severity;
    private String storeId;
    private double avgPickupTimeMinutes;
    private int orderCount;
    private Instant windowStart;
    private Instant windowEnd;
    private Instant detectedAt;
    private int schemaVersion;

    public OpsAlert() {}

    public static OpsAlert bottleneck(
            String storeId,
            double avgPickupTimeMinutes,
            int orderCount,
            Instant windowStart,
            Instant windowEnd,
            String severity,
            Instant detectedAt
    ) {
        OpsAlert alert = new OpsAlert();
        alert.alertId = storeId + ":" + windowEnd.toEpochMilli() + ":BOTTLENECK";
        alert.alertType = "RESTAURANT_QUEUE_BOTTLENECK";
        alert.severity = severity;
        alert.storeId = storeId;
        alert.avgPickupTimeMinutes = avgPickupTimeMinutes;
        alert.orderCount = orderCount;
        alert.windowStart = windowStart;
        alert.windowEnd = windowEnd;
        alert.detectedAt = detectedAt;
        alert.schemaVersion = 1;
        return alert;
    }

    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }

    public String getAlertType() { return alertType; }
    public void setAlertType(String alertType) { this.alertType = alertType; }

    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }

    public String getStoreId() { return storeId; }
    public void setStoreId(String storeId) { this.storeId = storeId; }

    public double getAvgPickupTimeMinutes() { return avgPickupTimeMinutes; }
    public void setAvgPickupTimeMinutes(double avgPickupTimeMinutes) { this.avgPickupTimeMinutes = avgPickupTimeMinutes; }

    public int getOrderCount() { return orderCount; }
    public void setOrderCount(int orderCount) { this.orderCount = orderCount; }

    public Instant getWindowStart() { return windowStart; }
    public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

    public Instant getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }

    public Instant getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Instant detectedAt) { this.detectedAt = detectedAt; }

    public int getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(int schemaVersion) { this.schemaVersion = schemaVersion; }
}
