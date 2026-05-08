package com.company.delayedordersms.model;

public enum OrderStatus {
    CREATED,
    ACCEPTED,
    PICKED_UP,
    DELIVERED,
    CANCELLED;

    public boolean isTerminal() {
        return this == DELIVERED || this == CANCELLED;
    }

    public boolean isActive() {
        return !isTerminal();
    }
}