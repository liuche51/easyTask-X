package com.github.liuche51.easyTaskX.cluster;

import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 系统配置项
 */
public class EasyTaskConfig {
    private static final Logger log = LoggerFactory.getLogger(EasyTaskConfig.class);
    /**
     * zk地址。必填 如:127.0.0.1:2181,192.168.1.128:2181
     */
    private String zkAddress;
    /**
     * 任务备份数量，默认2。最大2，超过部分无效
     */
    private int backupCount = 2;
    /**
     * 自定义任务本地存储路径。必填
     */
    private String taskStorePath;
    /**
     * sqlite连接池大小设置。默认3
     */
    private int sQLlitePoolSize = 3;
    /**
     * Netty客户端连接池大小设置。默认3
     */
    private int nettyPoolSize = 3;
    /**
     * 设置当前节点Netty服务端口号。默认2020
     */
    private int serverPort = 2020;
    /**
     * 设置集群Netty通信调用超时时间。默认30秒
     */
    private int timeOut = 30;
    /**
     * ZK节点信息失效超时时间。默认超过60s就判断为失效节点，任何其他节点可删除掉
     */
    private int loseTimeOut = 60;
    /**
     * ZK节点信息死亡超时时间。默认超过30s就判断为Leader失效节点，其Follow节点可进入选举新Leader
     */
    private int deadTimeOut = 30;
    /**
     * 节点对zk的心跳频率。默认2s一次
     */
    private int heartBeat = 2;
    /**
     * 集群节点之间通信失败重试次数。默认2次
     */
    private int tryCount = 2;
    /**
     * 清理任务备份表中失效的leader备份。默认1小时一次。单位毫秒
     */
    private int clearScheduleBakTime = 36500000;
    /**
     * 集群公用程池
     */
    private ExecutorService clusterPool = null;
    private String clusterPool_corePoolSize;
    private String clusterPool_maximumPoolSize;
    private String clusterPool_keepAliveTime;

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public void setBackupCount(int backupCount) throws Exception {
        if (backupCount != 2)
            throw new Exception("backupCount != 2");
        else this.backupCount = backupCount;
    }

    public String getTaskStorePath() {
        return taskStorePath;
    }

    /**
     * set Task Store Path.example  C:/db
     *
     * @param path
     * @throws Exception
     */
    public void setTaskStorePath(String path) throws Exception {
        this.taskStorePath = path;
    }

    public int getsQLlitePoolSize() {
        return sQLlitePoolSize;
    }

    /**
     * set SQLlitePool Size，default qty 3
     *
     * @param sQLlitePoolSize
     * @throws Exception
     */
    public void setSQLlitePoolSize(int sQLlitePoolSize) throws Exception {
        if (sQLlitePoolSize < 1)
            throw new Exception("sQLlitePoolSize must >1");
        this.sQLlitePoolSize = sQLlitePoolSize;
    }

    public int getNettyPoolSize() {
        return nettyPoolSize;
    }

    public void setNettyPoolSize(int nettyPoolSize) throws Exception {
        if (nettyPoolSize < 1)
            throw new Exception("nettyPoolSize must >1");
        this.nettyPoolSize = nettyPoolSize;
    }

    public String getAddress() throws UnknownHostException {
        StringBuffer buffer = new StringBuffer(Util.getLocalIP());
        buffer.append(":").append(getServerPort());
        return buffer.toString();
    }

    public int getServerPort() {
        return serverPort;
    }

    /**
     * set ServerPort，default 2020
     *
     * @param port
     * @throws Exception
     */
    public void setServerPort(int port) throws Exception {
        if (port == 0)
            throw new Exception("ServerPort must not empty");
        this.serverPort = port;
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

    public int getDeadTimeOut() {
        return deadTimeOut;
    }

    public void setDeadTimeOut(int deadTimeOut) {
        this.deadTimeOut = deadTimeOut;
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

    public ExecutorService getClusterPool() {
        return clusterPool;
    }

    /**
     * 设置集群总线程池
     *
     * @param clusterPool
     * @throws Exception
     */
    public void setClusterPool(ThreadPoolExecutor clusterPool) throws Exception {
        this.clusterPool = clusterPool;
    }

    /**
     * 必填项验证
     * @param config
     * @throws Exception
     */
    public static void validateNecessary(EasyTaskConfig config) throws Exception {
        if(StringUtils.isNullOrEmpty(config.zkAddress))
            throw new Exception("zkAddress is necessary!");
        if(StringUtils.isNullOrEmpty(config.taskStorePath))
            throw new Exception("taskStorePath is necessary!");
        if(config.clusterPool==null)
            config.clusterPool=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }
}
