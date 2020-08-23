package com.github.liuche51.easyTaskX;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import org.apache.log4j.PropertyConfigurator;
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

    /**
     * 开发环境配置启动带入参数：env dev  以便直接加载resource下的配置资源
     * 生产环境配置资源文件外置
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (!Util.isDevEnvironment(args))
            initLogConfig();
        EasyTaskConfig config = loadConfig(args);
        log.info("AnnularQueue start config EasyTaskConfig=" + JSONObject.toJSONString(config));
        ClusterService.start(config);
        log.info("===============================================================================");
        log.info("================== EasyTask-X Started Successfull！=============================");
        log.info("================================================================================");
        while (true) {
            Thread.sleep(1000);
        }
    }

    /**
     * 使用指定配置文件初始化日志
     *
     * @throws IOException
     */
    private static void initLogConfig() throws IOException {
        File directory = new File("");// 参数为空
        String courseFile = directory.getCanonicalPath();//获取程序运行的根目录
        PropertyConfigurator.configure(courseFile + "/log4j.properties");
    }

    private static EasyTaskConfig loadConfig(String[] args) throws Exception {
        Properties properties = new Properties();
        if (!Util.isDevEnvironment(args)) {
            File directory = new File("");// 参数为空
            String courseFile = directory.getCanonicalPath();//获取程序运行的根目录
            //可以读取当前程序根目录下的文件
            BufferedReader bufferedReader = new BufferedReader(new FileReader(courseFile + "/easyTaskX.cfg"));
            properties.load(bufferedReader);
        } else {
            //开发环境可以直接读取类路径下的文件。resource中的。开发调试使用
            InputStream in = EasyTaskXApp.class.getClassLoader().getResourceAsStream("easyTaskX.cfg");
            properties.load(in);
        }
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
        if (!StringUtils.isNullOrEmpty(clusterPool_corePoolSize) && !StringUtils.isNullOrEmpty(clusterPool_maximumPoolSize) && !StringUtils.isNullOrEmpty(clusterPool_keepAliveTime)) {
            ThreadPoolExecutor cluster = new ThreadPoolExecutor(Integer.parseInt(clusterPool_corePoolSize), Integer.parseInt(clusterPool_maximumPoolSize), Integer.parseInt(clusterPool_keepAliveTime), TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
            config.setClusterPool(cluster);
        }
        return config;
    }

}
