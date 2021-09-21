package com.github.liuche51.easyTaskX.enume;

/**
 * TCC事务状态
 */
public class TransactionStatusEnum {
    /**
     * 第一阶段
     */
    public static final short TRIED=1;
    /**
     * 第二阶段
     */
    public static final short CONFIRM=2;
    /**
     * 已完成
     */
    public static final short FINISHED=4;
}
