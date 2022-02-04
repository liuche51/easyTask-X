package com.github.liuche51.easyTaskX.zk;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZKUtil {
    //会话超时时间
    private static  int SESSION_TIMEOUT = 30 * 1000;
    //连接超时时间
    private static int CONNECTION_TIMEOUT = 3 * 1000;

    //创建连接实例
    private static CuratorFramework client = null;
    public static CuratorFramework getClient(){
        if(client!=null)
            return client;
        //1 重试策略：初试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //2 通过工厂创建连接
         client = CuratorFrameworkFactory.builder()
                .connectString(BrokerService.getConfig().getZkAddress()).connectionTimeoutMs(CONNECTION_TIMEOUT)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .namespace("easyTask-X")//命名空间
                .build();
        //3 开启连接
        client.start();
        //4 创建注册二级节点目录
        //ZKService.createZKNode(StringConstant.SERVER);
        return client;
    }

}
