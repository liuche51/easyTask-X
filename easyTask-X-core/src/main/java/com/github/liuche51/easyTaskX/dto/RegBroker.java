package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 注册表用服务端节点对象
 */
public class RegBroker extends BaseNode{
    /**
     * 最近一次心跳时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime lastHeartbeat;
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime createTime;
    /**
     * 当前节点的所有follows
     */
    private ConcurrentHashMap<String, RegNode> slaves = new ConcurrentHashMap();
    /**
     * 当前节点的所有leader
     */
    private ConcurrentHashMap<String, RegNode> masters = new ConcurrentHashMap<>();
    /**
     * 重新分配任务至新Client数据同步状态
     */
    private Short ReDispatchTaskStatus= NodeSyncDataStatusEnum.SUCCEEDED ;
    public RegBroker(BaseNode baseNode){
        super(baseNode.getHost(), baseNode.getPort());
    }
    public RegBroker(String host, int port) {
        super(host, port);
    }
    public RegBroker(String address) {
        super(address);
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

    public Short getReDispatchTaskStatus() {
        return ReDispatchTaskStatus;
    }

    public void setReDispatchTaskStatus(Short reDispatchTaskStatus) {
        ReDispatchTaskStatus = reDispatchTaskStatus;
    }
}
