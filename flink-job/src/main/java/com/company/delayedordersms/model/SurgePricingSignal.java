package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class SurgePricingSignal implements Serializable {

    private String signalId;
    private String signalType;
    private String zoneId;
    private double surgeMultiplier;
    private double demandFactor;
    private double weatherFactor;
    private String weatherCondition;
    private int ordersInWindow;
    private int atRiskOrderCount;
    private Instant emittedAt;
    private int schemaVersion;

    public SurgePricingSignal() {}

    public static SurgePricingSignal surge(
            String zoneId,
            long windowEndMs,
            double surgeMultiplier,
            double demandFactor,
            double weatherFactor,
            String weatherCondition,
            int ordersInWindow,
            int atRiskOrderCount,
            Instant emittedAt
    ) {
        SurgePricingSignal s = new SurgePricingSignal();
        s.signalId = zoneId + ":" + windowEndMs;
        s.signalType = "SURGE_PRICING";
        s.zoneId = zoneId;
        s.surgeMultiplier = surgeMultiplier;
        s.demandFactor = demandFactor;
        s.weatherFactor = weatherFactor;
        s.weatherCondition = weatherCondition;
        s.ordersInWindow = ordersInWindow;
        s.atRiskOrderCount = atRiskOrderCount;
        s.emittedAt = emittedAt;
        s.schemaVersion = 1;
        return s;
    }

    public String getSignalId() { return signalId; }
    public void setSignalId(String signalId) { this.signalId = signalId; }

    public String getSignalType() { return signalType; }
    public void setSignalType(String signalType) { this.signalType = signalType; }

    public String getZoneId() { return zoneId; }
    public void setZoneId(String zoneId) { this.zoneId = zoneId; }

    public double getSurgeMultiplier() { return surgeMultiplier; }
    public void setSurgeMultiplier(double surgeMultiplier) { this.surgeMultiplier = surgeMultiplier; }

    public double getDemandFactor() { return demandFactor; }
    public void setDemandFactor(double demandFactor) { this.demandFactor = demandFactor; }

    public double getWeatherFactor() { return weatherFactor; }
    public void setWeatherFactor(double weatherFactor) { this.weatherFactor = weatherFactor; }

    public String getWeatherCondition() { return weatherCondition; }
    public void setWeatherCondition(String weatherCondition) { this.weatherCondition = weatherCondition; }

    public int getOrdersInWindow() { return ordersInWindow; }
    public void setOrdersInWindow(int ordersInWindow) { this.ordersInWindow = ordersInWindow; }

    public int getAtRiskOrderCount() { return atRiskOrderCount; }
    public void setAtRiskOrderCount(int atRiskOrderCount) { this.atRiskOrderCount = atRiskOrderCount; }

    public Instant getEmittedAt() { return emittedAt; }
    public void setEmittedAt(Instant emittedAt) { this.emittedAt = emittedAt; }

    public int getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(int schemaVersion) { this.schemaVersion = schemaVersion; }
}
