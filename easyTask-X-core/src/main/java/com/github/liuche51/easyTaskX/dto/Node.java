package com.github.liuche51.easyTaskX.dto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点对象
 */
public class Node extends BaseNode {
    /**
     * 当前节点的所有clients
     */
    private ConcurrentHashMap<String, Node> clients = new ConcurrentHashMap<String, Node>();
    /**
     * 当前节点的所有follows
     */
    private ConcurrentHashMap<String, Node> slaves = new ConcurrentHashMap<String, Node>();
    /**
     * 当前节点的所有leader
     */
    private ConcurrentHashMap<String, Node> masters = new ConcurrentHashMap<>();
    /**
     * leader
     */
    private Node clusterLeader=null;
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

    public ConcurrentHashMap<String, Node> getClients() {
        return clients;
    }

    public void setClients(ConcurrentHashMap<String, Node> clients) {
        this.clients = clients;
    }

    public ConcurrentHashMap<String, Node> getSlaves() {
        return slaves;
    }

    public void setSlaves(ConcurrentHashMap<String, Node> slaves) {
        this.slaves = slaves;
    }

    public ConcurrentHashMap<String, Node> getMasters() {
        return masters;
    }

    public void setMasters(ConcurrentHashMap<String, Node> masters) {
        this.masters = masters;
    }

    public Node getClusterLeader() {
        return clusterLeader;
    }

    public void setClusterLeader(Node clusterLeader) {
        this.clusterLeader = clusterLeader;
    }

    public String getBakLeader() {
        return bakLeader;
    }

    public void setBakLeader(String bakLeader) {
        this.bakLeader = bakLeader;
    }
}
