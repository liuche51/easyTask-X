package com.github.liuche51.easyTaskX.cluster;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class AdvanceConfig {
    /**
     * sqlite连接池大小设置。默认3
     */
    private int dbPoolSize = 3;
    /**
     * Netty客户端连接池大小设置。默认3
     */
    private int nettyPoolSize = 3;
    /**
     * 设置集群Netty通信调用超时时间。默认30秒
     */
    private int timeOut = 30;
    /**
     * 集群节点失效判定时间。单位分钟
     */
    private int loseTimeOut = 5;
    /**
     * 节点对leader的心跳频率。单位秒
     */
    private int heartBeat = 30;
    /**
     * 集群节点之间通信失败重试次数。默认2次
     */
    private int tryCount = 2;
    /**
     * 是否debug模式。设置为TRUE，就可以看到完整日志跟踪信息
     */
    private boolean debug=false;
    /**
     * binlog同步一次数据量。
     */
    private int binlogCount = 100;
    /**
     * Client的失效Broker将任务重新分配给新Client的批次大小。默认5个任务一批次
     */
    private int reDispatchBatchCount = 5;
    /**
     * Leader收集集群节点心跳队列的最大容量。
     * 1、此队列供bakleader异步同步使用。每个bakleader的队列都是单独的
     * 2、队列采用有界，防止内存溢出
     */
    private int followsHeartbeatsQueueCapacity=5000;
    /**
     * 处理任务队列最大长度
     */
    private int taskQueueCapacity=10000;
    /**
     * 清理任务备份表中失效的leader备份。默认1小时一次。
     */
    private int clearScheduleBakTime = 1;
    /**
     * Folow节点从leader更新注册表信息间隔时间。单位分钟。
     */
    private int followUpdateRegeditTime = 5;
    /**
     * bakleader节点从leader更新注册表信息间隔时间。单位分钟。
     */
    private int bakLeaderUpdateRegeditTime = 10;
    /**
     * 从leader更新Clients列表间隔时间。单位小时。
     */
    private int updateClientsTime = 1;
    /**
     * 集群公用程池
     */
    private ExecutorService clusterPool = null;
    private String clusterPool_corePoolSize;
    private String clusterPool_maximumPoolSize;
    private String clusterPool_keepAliveTime;

    public int getsQLlitePoolSize() {
        return dbPoolSize;
    }

    /**
     * set SQLlitePool Size，default qty
     *
     * @param dbPoolSize
     * @throws Exception
     */
    public void setSQLlitePoolSize(int dbPoolSize) throws Exception {
        if (dbPoolSize < 1)
            throw new Exception("sQLlitePoolSize must >1");
        this.dbPoolSize = dbPoolSize;
    }

    public int getNettyPoolSize() {
        return nettyPoolSize;
    }

    public void setNettyPoolSize(int nettyPoolSize) throws Exception {
        if (nettyPoolSize < 1)
            throw new Exception("nettyPoolSize must >1");
        this.nettyPoolSize = nettyPoolSize;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public int getLoseTimeOut() {
        return loseTimeOut;
    }

    public void setLoseTimeOut(int loseTimeOut) {
        this.loseTimeOut = loseTimeOut;
    }

    public int getHeartBeat() {
        return heartBeat;
    }

    public void setHeartBeat(int heartBeat) {
        this.heartBeat = heartBeat;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public int getBinlogCount() {
        return binlogCount;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void setBinlogCount(int binlogCount) {
        this.binlogCount = binlogCount;
    }

    public int getReDispatchBatchCount() {
        return reDispatchBatchCount;
    }

    public void setReDispatchBatchCount(int reDispatchBatchCount) {
        this.reDispatchBatchCount = reDispatchBatchCount;
    }

    public int getFollowsHeartbeatsQueueCapacity() {
        return followsHeartbeatsQueueCapacity;
    }

    public void setFollowsHeartbeatsQueueCapacity(int followsHeartbeatsQueueCapacity) {
        this.followsHeartbeatsQueueCapacity = followsHeartbeatsQueueCapacity;
    }

    public int getTaskQueueCapacity() {
        return taskQueueCapacity;
    }

    public void setTaskQueueCapacity(int taskQueueCapacity) {
        this.taskQueueCapacity = taskQueueCapacity;
    }

    public int getClearScheduleBakTime() {
        return clearScheduleBakTime;
    }

    public void setClearScheduleBakTime(int clearScheduleBakTime) {
        this.clearScheduleBakTime = clearScheduleBakTime;
    }

    public int getFollowUpdateRegeditTime() {
        return followUpdateRegeditTime;
    }

    public void setFollowUpdateRegeditTime(int followUpdateRegeditTime) {
        this.followUpdateRegeditTime = followUpdateRegeditTime;
    }

    public int getBakLeaderUpdateRegeditTime() {
        return bakLeaderUpdateRegeditTime;
    }

    public void setBakLeaderUpdateRegeditTime(int bakLeaderUpdateRegeditTime) {
        this.bakLeaderUpdateRegeditTime = bakLeaderUpdateRegeditTime;
    }

    public int getUpdateClientsTime() {
        return updateClientsTime;
    }

    public void setUpdateClientsTime(int updateClientsTime) {
        this.updateClientsTime = updateClientsTime;
    }

    public ExecutorService getClusterPool() {
        return clusterPool;
    }

    public void setClusterPool(ExecutorService clusterPool) {
        this.clusterPool = clusterPool;
    }
}
