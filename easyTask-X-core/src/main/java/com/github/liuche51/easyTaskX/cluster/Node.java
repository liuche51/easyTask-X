package com.github.liuche51.easyTaskX.cluster;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点对象
 */
public class Node implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Node.class);
    private String host = "";
    private int port = ClusterService.getConfig().getServerPort();
    /**
     * 数据一致性状态。
     */
    private Short dataStatus = NodeSyncDataStatusEnum.SYNC;
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
     * 集群leader
     */
    private Node clusterLeader=null;
    public Node(String host, int port,boolean simple) {
        this.host = host;
        this.port = port;
        if(simple){
            this.dataStatus=null;
            this.clients=null;
            this.follows=null;
            this.leaders=null;
        }
    }
    public Node(String host, int port) {
        this.host = host;
        this.port = port;
    }
    public Node(String host, int port,Short dataStatus) {
        this.host = host;
        this.port = port;
        this.dataStatus=dataStatus;
    }
    public Node(String address) {
        String[] ret=address.split(":");
        this.host = ret[0];
        this.port = Integer.parseInt(ret[1]);
    }
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Short getDataStatus() {
        return dataStatus;
    }

    public void setDataStatus(Short dataStatus) {
        this.dataStatus = dataStatus;
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

    @JSONField(serialize = false)
    public String getAddress() {
        StringBuffer str = new StringBuffer(this.host);
        str.append(":").append(this.port);
        return str.toString();
    }
    @JSONField(serialize = false)
    public NettyClient getClient() throws InterruptedException {
        return NettyConnectionFactory.getInstance().getConnection(host, port);
    }

    /**
     * 获取Netty客户端连接。带重试次数
     * 目前一次通信，占用一个Netty连接。
     * @param tryCount
     * @return
     */
    @JSONField(serialize = false)
    public NettyClient getClientWithCount(int tryCount) throws Exception {
        if (tryCount == 0) throw new Exception("getClientWithCount()-> exception!");
        try {
            return getClient();
        } catch (Exception e) {
            log.info("getClientWithCount tryCount=" + tryCount + ",objectHost="+this.getAddress());
            log.error("getClientWithCount()-> exception!", e);
            return getClientWithCount(tryCount);
        } finally {
            tryCount--;
        }
    }
}
