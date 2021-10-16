package com.github.liuche51.easyTaskX.dto;

import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;

/**
 * 注册表用节点对象
 */
public class RegNode extends BaseNode {
    /**
     * 数据一致性状态。0同步中，1已同步
     */
    private Short dataStatus=Short.valueOf("0");;

    public RegNode(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public RegNode(String host, int port, Short dataStatus) {
        super(host, port);
        this.dataStatus=dataStatus;
    }

    public RegNode(String host, int port) {
        super(host, port);
    }

    public RegNode(String address) {
        super(address);
    }

    public Short getDataStatus() {
        return dataStatus;
    }

    public void setDataStatus(Short dataStatus) {
        this.dataStatus = dataStatus;
    }
}
