package com.github.liuche51.easyTaskX.dto;

public class SlaveNode extends BaseNode {
    /**
     * slave同步当前master的binlog位置
     * 1、master使用
     */
    private long currentBinlogIndex = 0;

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
}
