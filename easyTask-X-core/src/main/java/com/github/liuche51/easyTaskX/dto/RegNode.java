package com.github.liuche51.easyTaskX.dto;

import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;

/**
 * 注册表用节点对象
 */
public class RegNode extends BaseNode {
    /**
     * 数据一致性状态。
     */
    private Short dataStatus;

    public RegNode(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public RegNode(String host, int port, Short dataStatus) {
        super(host, port);
    }

    public RegNode(String host, int port) {
        super(host, port);
    }

    public RegNode(String address) {
        super(address);
    }

}
