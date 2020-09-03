package com.github.liuche51.easyTaskX.dto;

import com.github.liuche51.easyTaskX.cluster.Node;

import java.util.Date;

public class RegisterNode {
    private Node node;
    /**
     * 最近一次心跳时间
     */
    private Date lastHeartbeat;
    private Date createTime;

    public RegisterNode(Node node){
        this.node=node;
        this.createTime=new Date();
    }
    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Date lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
