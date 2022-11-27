package com.github.liuche51.easyTaskX.enume;

/**
 * 立即执行类型
 */
public enum ImmediatelyTypeEnum {
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

    private ImmediatelyTypeEnum(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ImmediatelyTypeEnum getByValue(String value) {
        switch (value) {
            case "0":
                return ImmediatelyTypeEnum.NONE;
            case "1":
                return ImmediatelyTypeEnum.LOCAL;
            case "2":
                return ImmediatelyTypeEnum.DISTRIB;
            default:
                return null;
        }
    }
}
