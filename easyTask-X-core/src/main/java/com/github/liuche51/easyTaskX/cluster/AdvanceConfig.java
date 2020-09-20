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
     * 集群节点失效判定时间。默认10s
     */
    private int loseTimeOut = 10;
    /**
     * 节点对zk的心跳频率。默认3s一次
     */
    private int heartBeat = 3;
    /**
     * 集群节点之间通信失败重试次数。默认2次
     */
    private int tryCount = 2;
    /**
     * 清理任务备份表中失效的leader备份。默认1小时一次。单位毫秒
     */
    private int clearScheduleBakTime = 36500000;
    /**
     * 节点从集群leader更新注册表信息间隔时间。单位秒。
     */
    private int updateRegeditTime=300;
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
        return heartBeat * 1000;
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

    public int getClearScheduleBakTime() {
        return clearScheduleBakTime;
    }

    public void setClearScheduleBakTime(int clearScheduleBakTime) {
        this.clearScheduleBakTime = clearScheduleBakTime;
    }

    public int getUpdateRegeditTime() {
        return updateRegeditTime;
    }

    public void setUpdateRegeditTime(int updateRegeditTime) {
        this.updateRegeditTime = updateRegeditTime;
    }

    public ExecutorService getClusterPool() {
        return clusterPool;
    }

    public void setClusterPool(ExecutorService clusterPool) {
        this.clusterPool = clusterPool;
    }
}
