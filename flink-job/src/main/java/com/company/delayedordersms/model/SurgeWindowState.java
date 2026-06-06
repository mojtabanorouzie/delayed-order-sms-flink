package com.company.delayedordersms.model;

import java.io.Serializable;

public class SurgeWindowState implements Serializable {

    private long windowEndMs;
    private int ordersInWindow;
    private int atRiskOrderCount;

    public SurgeWindowState() {}

    public SurgeWindowState(long windowEndMs, int ordersInWindow, int atRiskOrderCount) {
        this.windowEndMs = windowEndMs;
        this.ordersInWindow = ordersInWindow;
        this.atRiskOrderCount = atRiskOrderCount;
    }

    public long getWindowEndMs() { return windowEndMs; }
    public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }

    public int getOrdersInWindow() { return ordersInWindow; }
    public void setOrdersInWindow(int ordersInWindow) { this.ordersInWindow = ordersInWindow; }

    public int getAtRiskOrderCount() { return atRiskOrderCount; }
    public void setAtRiskOrderCount(int atRiskOrderCount) { this.atRiskOrderCount = atRiskOrderCount; }
}
