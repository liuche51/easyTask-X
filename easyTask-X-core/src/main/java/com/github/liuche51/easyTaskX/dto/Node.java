package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZonedDateTime;
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
    private ConcurrentHashMap<String, Node> follows = new ConcurrentHashMap<String, Node>();
    /**
     * 当前节点的所有leader
     */
    private ConcurrentHashMap<String, Node> leaders = new ConcurrentHashMap<>();
    /**
     * leader
     */
    private Node clusterLeader=null;
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

    public ConcurrentHashMap<String, Node> getFollows() {
        return follows;
    }

    public void setFollows(ConcurrentHashMap<String, Node> follows) {
        this.follows = follows;
    }

    public ConcurrentHashMap<String, Node> getLeaders() {
        return leaders;
    }

    public void setLeaders(ConcurrentHashMap<String, Node> leaders) {
        this.leaders = leaders;
    }

    public Node getClusterLeader() {
        return clusterLeader;
    }

    public void setClusterLeader(Node clusterLeader) {
        this.clusterLeader = clusterLeader;
    }
}
