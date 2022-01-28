package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTaskX.enume.DataStatusEnum;
import com.github.liuche51.easyTaskX.enume.NodeStatusEnum;

import java.time.ZonedDateTime;

/**
 * 注册表用节点对象
 */
public class RegNode extends BaseNode {
    /**
     * 最近一次心跳时间
     */
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime lastHeartbeat;
    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime createTime;
    /**
     * 数据状态。
     * 1、正常状态0，slave数据异步同步master数据跟随正常。没有什么延迟
     * 2、未同步完成状态9，salve当前数据异步同步master存在较大延迟。
     */
    private volatile int dataStatus = DataStatusEnum.NORMAL;
    /**
     * 节点状态。
     * 1、正常状态0，表示当前节点处于正常接受处理任务的状态。
     * 2、恢复状态9，节点关系有调整，需要恢复数据一致性。
     * 此时节点某些操作会被限制，直到恢复到正常状态。
     * ①比如slave被选为新master，则其定时清除备份库数据就不能执行。新选出的salve，需要重新从master同步数据，为了节约时间，这期间需要拿到master快照进行恢复。
     * ②client宕机，其上执行的任务
     */
    private volatile int nodeStatus = NodeStatusEnum.NORMAL;

    public RegNode(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public RegNode(String host, int port) {
        super(host, port);
    }

    public RegNode(String address) {
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

    public Integer getDataStatus() {
        return dataStatus;
    }

    public void setDataStatus(Integer dataStatus) {
        this.dataStatus = dataStatus;
    }

    public int getNodeStatus() {
        return nodeStatus;
    }

    public void setNodeStatus(int nodeStatus) {
        this.nodeStatus = nodeStatus;
    }
}
