package com.company.delayedordersms.model;

import java.io.Serializable;

public class RestaurantWindowState implements Serializable {

    private long windowEndMs;
    private int completedOrderCount;
    private long sumPickupMillis;

    public RestaurantWindowState() {}

    public RestaurantWindowState(long windowEndMs, int completedOrderCount, long sumPickupMillis) {
        this.windowEndMs = windowEndMs;
        this.completedOrderCount = completedOrderCount;
        this.sumPickupMillis = sumPickupMillis;
    }

    public long getWindowEndMs() { return windowEndMs; }
    public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }

    public int getCompletedOrderCount() { return completedOrderCount; }
    public void setCompletedOrderCount(int completedOrderCount) { this.completedOrderCount = completedOrderCount; }

    public long getSumPickupMillis() { return sumPickupMillis; }
    public void setSumPickupMillis(long sumPickupMillis) { this.sumPickupMillis = sumPickupMillis; }
}
