package com.github.liuche51.easyTaskX.dto;

import com.github.liuche51.easyTaskX.enume.DataStatusEnum;

public class SlaveNode extends BaseNode {
    /**
     * slave同步当前master的binlog位置
     * 1、master使用
     */
    private long currentBinlogIndex = 0;
    /**
     * slave当前的数据状态缓存
     * 1、master使用
     */
    private int lastDataStatus= DataStatusEnum.NORMAL;

    public SlaveNode(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }
    public SlaveNode(String address) {
        super(address);
    }
    public SlaveNode(String host, int port) {
        super(host, port);
    }

    public long getCurrentBinlogIndex() {
        return currentBinlogIndex;
    }

    public void setCurrentBinlogIndex(long currentBinlogIndex) {
        this.currentBinlogIndex = currentBinlogIndex;
    }

    public int getLastDataStatus() {
        return lastDataStatus;
    }

    public void setLastDataStatus(int lastDataStatus) {
        this.lastDataStatus = lastDataStatus;
    }
}
