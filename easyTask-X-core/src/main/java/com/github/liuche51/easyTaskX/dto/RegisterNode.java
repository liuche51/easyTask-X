package com.github.liuche51.easyTaskX.dto;

import com.github.liuche51.easyTaskX.cluster.Node;

import java.time.ZonedDateTime;
import java.util.Date;

public class RegisterNode {
    private Node node;
    /**
     * 最近一次心跳时间
     */
    private ZonedDateTime lastHeartbeat;
    private ZonedDateTime createTime;

    public RegisterNode(Node node){
        this.node=node;
        this.createTime=ZonedDateTime.now();
    }
    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public ZonedDateTime getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(ZonedDateTime lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public ZonedDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(ZonedDateTime createTime) {
        this.createTime = createTime;
    }
}
