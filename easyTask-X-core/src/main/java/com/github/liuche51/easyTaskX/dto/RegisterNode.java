package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.time.ZonedDateTime;

public class RegisterNode {
    private Node node;
    /**
     * 最近一次心跳时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime lastHeartbeat;
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
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
