package com.github.liuche51.easyTaskX.dto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 节点对象
 */
public class Node extends BaseNode {
    /**
     * 当前节点所有可用的clients
     */
    private CopyOnWriteArrayList<BaseNode> clients = new CopyOnWriteArrayList<BaseNode>();
    /**
     * 当前节点的所有slaves
     */
    private ConcurrentHashMap<String, BaseNode> slaves = new ConcurrentHashMap<String, BaseNode>();
    /**
     * 当前节点的所有masters
     */
    private ConcurrentHashMap<String, BaseNode> masters = new ConcurrentHashMap<>();
    /**
     * leader
     */
    private BaseNode clusterLeader=null;
    /**
     * 集群备用leader
     * 用来判断leader宕机后，是否参与竞选新leader。如果有值说明当前集群有备用leader，则自己不参与竞选
     * 让备用leader竞选
     */
    private String bakLeader;
    public Node(BaseNode baseNode){
        super(baseNode.getHost(), baseNode.getPort());
    }
    public Node(String host, int port) {
        super(host, port);
    }

    public Node(String address) {
        super(address);
    }

    public CopyOnWriteArrayList<BaseNode> getClients() {
        return clients;
    }

    public void setClients(CopyOnWriteArrayList<BaseNode> clients) {
        this.clients = clients;
    }

    public ConcurrentHashMap<String, BaseNode> getSlaves() {
        return slaves;
    }

    public void setSlaves(ConcurrentHashMap<String, BaseNode> slaves) {
        this.slaves = slaves;
    }

    public ConcurrentHashMap<String, BaseNode> getMasters() {
        return masters;
    }

    public void setMasters(ConcurrentHashMap<String, BaseNode> masters) {
        this.masters = masters;
    }

    public BaseNode getClusterLeader() {
        return clusterLeader;
    }

    public void setClusterLeader(BaseNode clusterLeader) {
        this.clusterLeader = clusterLeader;
    }

    public String getBakLeader() {
        return bakLeader;
    }

    public void setBakLeader(String bakLeader) {
        this.bakLeader = bakLeader;
    }
}
