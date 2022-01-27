package com.github.liuche51.easyTaskX.enume;

/**
 * 节点状态
 */
public class NodeStatusEnum {
    /**
     * 正常状态
     * 1、
     */
    public static final int NORMAL = 0;
    /**
     * 崩溃恢复中
     * 1、表示节点当前处于崩溃恢复中。不允许做一些其他更新操作。保证数据一致性
     */
    public static final int RECOVERING = 9;
}
