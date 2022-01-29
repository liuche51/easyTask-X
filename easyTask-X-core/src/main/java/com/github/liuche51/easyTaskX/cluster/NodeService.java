package com.github.liuche51.easyTaskX.cluster;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.leader.BakLeaderService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.master.ClearDataTask;
import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.db.HistorySchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.socket.CmdServer;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class NodeService {
    private static EasyTaskConfig config = null;
    public static volatile boolean IS_STARTED = false;//是否已经启动
    public static volatile boolean IS_FIRST_STARTED = true;//当前节点是否属于首次启动注册到leader。默认是
    /**
     * 集群所有可用的clients
     */
    private ConcurrentHashMap<String, BaseNode> clients = new ConcurrentHashMap<String, BaseNode>();
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENT_NODE;

    /**
     * 集群一次性任务线程集合。
     * 系统没有重启只是初始化了集群initCURRENT_NODE()。此时也需要立即停止运行的一次性后台任务
     * 需要定时检查其中的线程是否已经运行完，完了需要移除线程对象，释放内存资源
     */
    public static List<OnceTask> onceTasks = new LinkedList<OnceTask>();
    /**
     * 集群定时任务线程集合。
     * 系统没有重启只是初始化了集群initCURRENT_NODE()。此时需要停止之前的定时任务，重新启动新的
     */
    public static List<TimerTask> timerTasks = new LinkedList<TimerTask>();

    public static EasyTaskConfig getConfig() {
        return config;
    }


    public static void setConfig(EasyTaskConfig config) {
        NodeService.config = config;
    }

    public ConcurrentHashMap<String, BaseNode> getClients() {
        return clients;
    }

    public void setClients(ConcurrentHashMap<String, BaseNode> clients) {
        this.clients = clients;
    }

    /**
     * 启动节点。
     * 线程互斥
     *
     * @param config
     * @throws Exception
     */
    public static synchronized void start(EasyTaskConfig config) throws Exception {
        //避免重复执行
        if (IS_STARTED)
            return;
        if (config == null)
            throw new Exception("config is null,please set a EasyTaskConfig!");
        EasyTaskConfig.validateNecessary(config);
        NodeService.config = config;
        DbInit.init();//初始化数据库
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        CmdServer.init();//启动命令服务的socket端口
        initCURRENT_NODE(true);//初始化本节点的集群服务
        IS_STARTED = true;
    }

    /**
     * 初始化当前节点的集群。
     * (系统重启或因心网络问题被leader踢出，然后又恢复了)
     * @param isFirstStarted 是否首次初始化。进程重启属于首次
     * @return
     */
    public static void initCURRENT_NODE(boolean isFirstStarted) throws Exception {
        clearThreadTask();
        if (isFirstStarted&&!NodeUtil.isAliveInCluster())
            NodeUtil.clearAllData();
        CURRENT_NODE = new Node(Util.getLocalIP(), NodeService.getConfig().getServerPort());
        timerTasks.add(BrokerService.startHeartBeat());
        timerTasks.add(clearDataTask());
        timerTasks.add(BrokerService.startUpdateRegeditTask());
        timerTasks.add(BrokerService.startBrokerUpdateClientsTask());
        timerTasks.add(BrokerService.startBrokerNotifyClientSubmitTaskResultTask());
        timerTasks.add(SlaveService.startScheduleBinLogSyncTask());
        timerTasks.add(SlaveService.startSlaveNotifyMasterSubmitTaskResultTask());
        timerTasks.add(MasterService.startMasterSubmitTask());
        timerTasks.add(MasterService.startMasterUpdateSubmitTaskStatusTask());
        timerTasks.add(MasterService.startMasterDeleteTaskTask());
        ZKService.listenLeaderDataNode();
    }


    public static TimerTask clearDataTask() {
        ClearDataTask task = new ClearDataTask();
        task.start();
        return task;
    }

    /**
     * 清理掉所有定时或后台线程任务
     */
    public static void clearThreadTask() {
        timerTasks.forEach(x -> {//先停止目前所有内部定时任务线程工作
            x.setExit(true);
        });
        timerTasks.clear();
        onceTasks.forEach(x -> {
            x.setExit(true);
        });
        onceTasks.clear();
    }
}
