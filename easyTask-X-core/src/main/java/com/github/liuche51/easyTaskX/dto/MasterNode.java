package com.github.liuche51.easyTaskX.dto;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master节点对象。
 * 1、目前用于slave对master的binlog订阅
 */
public class MasterNode extends BaseNode {
    /**
     * 当前master是否已经存在同步binlog的任务。
     */
    private volatile boolean syncing = false;
    /**
     * 当前已经同步日志的位置号。默认0，表示未开始
     */
    private long currentIndex = 0;

    public MasterNode(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public MasterNode(String host, int port) {
        super(host, port);
    }

    public MasterNode(String address) {
        super(address);
    }

    public boolean isSyncing() {
        return syncing;
    }

    public void setSyncing(boolean syncing) {
        this.syncing = syncing;
    }

    public long getCurrentIndex() {
        return currentIndex;
    }

    public void setCurrentIndex(long currentIndex) {
        this.currentIndex = currentIndex;
    }
}
