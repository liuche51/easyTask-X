package com.github.liuche51.easyTaskX.enume;

public enum TimeUnit {
    DAYS("DAYS"), HOURS("HOURS"), MINUTES("MINUTES"), SECONDS("SECONDS");
    private String value;

    private TimeUnit(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static TimeUnit getByValue(String value) {
        switch (value) {
            case "DAYS":
                return TimeUnit.DAYS;
            case "HOURS":
                return TimeUnit.HOURS;
            case "MINUTES":
                return TimeUnit.MINUTES;
            case "SECONDS":
                return TimeUnit.SECONDS;
            default:
                return null;
        }
    }
}
