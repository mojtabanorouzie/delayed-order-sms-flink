package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

public class CourierCommand implements Serializable {

    private String commandId;
    private String commandType;
    private String courierId;
    private String action;
    private int activeOrderCount;
    private Instant detectedAt;
    private int schemaVersion;

    public CourierCommand() {}

    public static CourierCommand pause(String courierId, int activeOrderCount, Instant detectedAt) {
        CourierCommand cmd = new CourierCommand();
        cmd.commandId = courierId + ":PAUSE";
        cmd.commandType = "COURIER_PAUSE";
        cmd.courierId = courierId;
        cmd.action = "PAUSE";
        cmd.activeOrderCount = activeOrderCount;
        cmd.detectedAt = detectedAt;
        cmd.schemaVersion = 1;
        return cmd;
    }

    public static CourierCommand resume(String courierId, int activeOrderCount, Instant detectedAt) {
        CourierCommand cmd = new CourierCommand();
        cmd.commandId = courierId + ":RESUME";
        cmd.commandType = "COURIER_RESUME";
        cmd.courierId = courierId;
        cmd.action = "RESUME";
        cmd.activeOrderCount = activeOrderCount;
        cmd.detectedAt = detectedAt;
        cmd.schemaVersion = 1;
        return cmd;
    }

    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }

    public String getCommandType() { return commandType; }
    public void setCommandType(String commandType) { this.commandType = commandType; }

    public String getCourierId() { return courierId; }
    public void setCourierId(String courierId) { this.courierId = courierId; }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public int getActiveOrderCount() { return activeOrderCount; }
    public void setActiveOrderCount(int activeOrderCount) { this.activeOrderCount = activeOrderCount; }

    public Instant getDetectedAt() { return detectedAt; }
    public void setDetectedAt(Instant detectedAt) { this.detectedAt = detectedAt; }

    public int getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(int schemaVersion) { this.schemaVersion = schemaVersion; }
}
