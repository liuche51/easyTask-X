package com.github.liuche51.easyTaskX;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.core.AnnularQueue;
import com.github.liuche51.easyTaskX.core.EasyTaskConfig;
import com.github.liuche51.easyTaskX.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EasyTaskXApp {
    private static Logger log = LoggerFactory.getLogger(EasyTaskXApp.class);

    public static void main(String[] args) throws Exception {
        EasyTaskConfig config = loadConfig();
        log.info("AnnularQueue start config EasyTaskConfig=" + JSONObject.toJSONString(config));
        AnnularQueue.getInstance().start(config);
        log.info("===============================================================================");
        log.info("================== EasyTask-X Started Successfull！=============================");
        log.info("================================================================================");
        while (true) {
            Thread.sleep(1000);
        }
    }

    private static EasyTaskConfig loadConfig() throws Exception {
        Properties properties = new Properties();
        File directory = new File("");// 参数为空
        String courseFile = directory.getCanonicalPath();
        //可以直接读取类路径下的文件。resource中的
     /*   InputStream in = EasyTaskXApp.class.getClassLoader().getResourceAsStream("easyTaskX.cfg");
        properties.load(in);*/
        //可以读取当前程序根目录下的文件
        BufferedReader bufferedReader = new BufferedReader(new FileReader(courseFile + "/easyTaskX.cfg"));
        properties.load(bufferedReader);
        //必填配置
        String zkAddress = properties.getProperty("zkAddress");
        String taskStorePath = properties.getProperty("taskStorePath");
        //以下是可选配置
        String serverPort = properties.getProperty("serverPort");
        String backupCount = properties.getProperty("backupCount");
        String sQLlitePoolSize = properties.getProperty("sQLlitePoolSize");
        String nettyPoolSize = properties.getProperty("nettyPoolSize");
        String timeOut = properties.getProperty("timeOut");
        String loseTimeOut = properties.getProperty("loseTimeOut");
        String deadTimeOut = properties.getProperty("deadTimeOut");
        String heartBeat = properties.getProperty("heartBeat");
        String tryCount = properties.getProperty("tryCount");
        String clearScheduleBakTime = properties.getProperty("clearScheduleBakTime");
        String dispatchsPool_corePoolSize = properties.getProperty("dispatchsPool.corePoolSize");
        String dispatchsPool_maximumPoolSize = properties.getProperty("dispatchsPool.maximumPoolSize");
        String dispatchsPool_keepAliveTime = properties.getProperty("dispatchsPool.keepAliveTime");
        String workersPool_corePoolSize = properties.getProperty("workersPool.corePoolSize");
        String workersPool_maximumPoolSize = properties.getProperty("workersPool.maximumPoolSize");
        String workersPool_keepAliveTime = properties.getProperty("workersPool.keepAliveTime");
        String clusterPool_corePoolSize = properties.getProperty("clusterPool.corePoolSize");
        String clusterPool_maximumPoolSize = properties.getProperty("clusterPool.maximumPoolSize");
        String clusterPool_keepAliveTime = properties.getProperty("clusterPool.keepAliveTime");
        EasyTaskConfig config = new EasyTaskConfig();
        config.setZkAddress(zkAddress);
        config.setTaskStorePath(taskStorePath);
        //以下是可选配置
        if (!StringUtils.isNullOrEmpty(serverPort))
            config.setServerPort(Integer.parseInt(serverPort));
        if (!StringUtils.isNullOrEmpty(backupCount))
            config.setBackupCount(Integer.parseInt(backupCount));
        if (!StringUtils.isNullOrEmpty(sQLlitePoolSize))
            config.setSQLlitePoolSize(Integer.parseInt(sQLlitePoolSize));
        if (!StringUtils.isNullOrEmpty(nettyPoolSize))
            config.setNettyPoolSize(Integer.parseInt(nettyPoolSize));
        if (!StringUtils.isNullOrEmpty(timeOut))
            config.setTimeOut(Integer.parseInt(timeOut));
        if (!StringUtils.isNullOrEmpty(loseTimeOut))
            config.setLoseTimeOut(Integer.parseInt(loseTimeOut));
        if (!StringUtils.isNullOrEmpty(deadTimeOut))
            config.setDeadTimeOut(Integer.parseInt(deadTimeOut));
        if (!StringUtils.isNullOrEmpty(heartBeat))
            config.setHeartBeat(Integer.parseInt(heartBeat));
        if (!StringUtils.isNullOrEmpty(tryCount))
            config.setTryCount(Integer.parseInt(tryCount));
        if (!StringUtils.isNullOrEmpty(clearScheduleBakTime))
            config.setClearScheduleBakTime(Integer.parseInt(clearScheduleBakTime));
        if (!StringUtils.isNullOrEmpty(dispatchsPool_corePoolSize) && !StringUtils.isNullOrEmpty(dispatchsPool_maximumPoolSize) && !StringUtils.isNullOrEmpty(dispatchsPool_keepAliveTime)) {
            ExecutorService dispatchs = new ThreadPoolExecutor(Integer.parseInt(dispatchsPool_corePoolSize), Integer.parseInt(dispatchsPool_maximumPoolSize), Integer.parseInt(dispatchsPool_keepAliveTime), TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
            config.setDispatchs(dispatchs);
        }
        if (!StringUtils.isNullOrEmpty(workersPool_corePoolSize) && !StringUtils.isNullOrEmpty(workersPool_maximumPoolSize) && !StringUtils.isNullOrEmpty(workersPool_keepAliveTime)) {
            ExecutorService workers = new ThreadPoolExecutor(Integer.parseInt(workersPool_corePoolSize), Integer.parseInt(workersPool_maximumPoolSize), Integer.parseInt(workersPool_keepAliveTime), TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
            config.setWorkers(workers);
        }
        if (!StringUtils.isNullOrEmpty(clusterPool_corePoolSize) && !StringUtils.isNullOrEmpty(clusterPool_maximumPoolSize) && !StringUtils.isNullOrEmpty(clusterPool_keepAliveTime)) {
            ExecutorService cluster = new ThreadPoolExecutor(Integer.parseInt(clusterPool_corePoolSize), Integer.parseInt(clusterPool_maximumPoolSize), Integer.parseInt(clusterPool_keepAliveTime), TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
            config.setWorkers(cluster);
        }
        return config;
    }

}
