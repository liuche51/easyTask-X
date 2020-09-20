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
     * 设置当前节点Netty服务端口号。默认2020
     */
    private int serverPort = 2020;
    /**
     * 设置当前节点CMD服务端口号。默认3030
     */
    private int cmdPort = 3030;
    /**
     * 高级配置项
     */
    private AdvanceConfig advanceConfig=new AdvanceConfig();

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


    public String getAddress() throws Exception {
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

    public int getCmdPort() {
        return cmdPort;
    }

    public void setCmdPort(int cmdPort) {
        this.cmdPort = cmdPort;
    }

    public AdvanceConfig getAdvanceConfig() {
        return advanceConfig;
    }

    public void setAdvanceConfig(AdvanceConfig advanceConfig) {
        this.advanceConfig = advanceConfig;
    }

    /**
     * 必填项验证
     *
     * @param config
     * @throws Exception
     */
    public static void validateNecessary(EasyTaskConfig config) throws Exception {
        if (StringUtils.isNullOrEmpty(config.zkAddress))
            throw new Exception("zkAddress is necessary!");
        if (StringUtils.isNullOrEmpty(config.taskStorePath))
            throw new Exception("taskStorePath is necessary!");
        if (config.getAdvanceConfig().getClusterPool() == null)
            config.getAdvanceConfig().setClusterPool(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2));
    }
}
