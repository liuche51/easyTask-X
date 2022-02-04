package com.github.liuche51.easyTaskX.dto;

/**
 * Master节点对象。
 * 1、目前用于slave对master的binlog订阅
 */
public class MasterNode extends BaseNode {
    /**
     * 当前salve所同步的当前master是否已经存在同步binlog的任务。
     * 1、防止多线程并发同步
     * 2、salve使用
     */
    private volatile boolean binlogSyncing = false;
    /**
     * 当前salve已经同步当前master日志的位置号。默认0，表示未开始
     * 1、salve使用
     */
    private long currentBinlogIndex = 0;

    public MasterNode(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public MasterNode(String host, int port) {
        super(host, port);
    }

    public MasterNode(String address) {
        super(address);
    }

    public boolean isBinlogSyncing() {
        return binlogSyncing;
    }

    public void setBinlogSyncing(boolean binlogSyncing) {
        this.binlogSyncing = binlogSyncing;
    }

    public long getCurrentBinlogIndex() {
        return currentBinlogIndex;
    }

    public void setCurrentBinlogIndex(long currentBinlogIndex) {
        this.currentBinlogIndex = currentBinlogIndex;
    }
}
