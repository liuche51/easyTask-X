package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 注册表用服务端节点对象
 */
public class RegClient extends RegNode {
    /**
     * 最近一次心跳时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime lastHeartbeat;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime createTime;

    public RegClient(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public RegClient(RegNode regNode) {
        super(regNode.getHost(), regNode.getPort(), regNode.getDataStatus());
    }

    public RegClient(String host, int port) {
        super(host, port);
    }

    public RegClient(String host, int port, Short dataStatus) {
        super(host, port, dataStatus);
    }

    public RegClient(String address) {
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
}
