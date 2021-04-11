package com.github.liuche51.easyTaskX.enume;

/**
 * 节点之间数据同步状态
 */
public class NodeSyncDataStatusEnum {
    /**
     * 未同步
     */
    public static final short UNSYNC=0;
    /**
     * 同步中
     */
    public static final short SYNCING=1;
    /**
     * 已同步
     */
    public static final short SUCCEEDED=2;
    /**
     * 同步失败
     */
    public static final short FAILED=9;
}
