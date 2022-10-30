package com.github.liuche51.easyTaskX.enume;

/**
 * 立即执行类型
 */
public enum ImmediatelyType {
    /**
     * 不需要立即执行
     */
    NONE(0),
    /**
     * 本地执行。
     */
    LOCAL(1),
    /**
     * 分布式执行
     * 1、需要提交到服务端进行二次分发任务执行。
     */
    DISTRIB(2);
    private int value;

    private ImmediatelyType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ImmediatelyType getByValue(String value) {
        switch (value) {
            case "0":
                return ImmediatelyType.NONE;
            case "1":
                return ImmediatelyType.LOCAL;
            case "2":
                return ImmediatelyType.DISTRIB;
            default:
                return null;
        }
    }
}
