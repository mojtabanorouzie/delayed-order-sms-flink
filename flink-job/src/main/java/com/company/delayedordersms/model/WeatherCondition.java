package com.company.delayedordersms.model;

public enum WeatherCondition {
    CLEAR, CLOUDY, RAIN, SNOW, UNKNOWN;

    public static WeatherCondition fromString(String s) {
        if (s == null) return UNKNOWN;
        try {
            return valueOf(s.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
