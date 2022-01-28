package com.github.liuche51.easyTaskX.dto;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 注册表用服务端节点对象
 */
public class RegBroker extends RegNode {

    /**
     * 当前节点的所有follows
     */
    private ConcurrentHashMap<String, RegNode> slaves = new ConcurrentHashMap();
    /**
     * 当前节点的所有leader
     */
    private ConcurrentHashMap<String, RegNode> masters = new ConcurrentHashMap<>();

    public RegBroker(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public RegBroker(RegNode regNode) {
        super(regNode.getHost(), regNode.getPort());
    }

    public RegBroker(String host, int port) {
        super(host, port);
    }

    public RegBroker(String host, int port, Integer dataStatus) {
        super(host, port, dataStatus);
    }

    public RegBroker(String address) {
        super(address);
    }

    public ConcurrentHashMap<String, RegNode> getSlaves() {
        return slaves;
    }

    public void setSlaves(ConcurrentHashMap<String, RegNode> slaves) {
        this.slaves = slaves;
    }

    public ConcurrentHashMap<String, RegNode> getMasters() {
        return masters;
    }

    public void setMasters(ConcurrentHashMap<String, RegNode> masters) {
        this.masters = masters;
    }

}
