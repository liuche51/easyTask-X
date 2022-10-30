package com.github.liuche51.easyTaskX.enume;

public enum TaskType {
    /**
     * execute once
     */
    ONECE(0),
    /**
     * execute mutilate
     */
    PERIOD(1);
    private int value;

    private TaskType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TaskType getByValue(String value) {
        switch (value) {
            case "0":
                return TaskType.ONECE;
            case "1":
                return TaskType.PERIOD;
            default:
                return null;
        }
    }
}
