package com.github.liuche51.easyTaskX.cluster.follow;

import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerNotifyClientSubmitTaskResultTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerRequestUpdateRegeditTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerUpdateClientsTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.HeartbeatsTask;
import com.github.liuche51.easyTaskX.cluster.task.master.ClearDataTask;
import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.SlaveNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.socket.CmdServer;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Broker
 */
public class BrokerService {

    /**
     * leader
     */
    public static BaseNode CLUSTER_LEADER = null;
    /**
     * 集群备用leader
     * 用来判断leader宕机后，是否参与竞选新leader。如果有值说明当前集群有备用leader，则自己不参与竞选
     * 让备用leader竞选
     */
    public static String BAK_LEADER;
    /**
     * 当前节点所有可用的clients
     */
    public static CopyOnWriteArrayList<BaseNode> CLIENTS = new CopyOnWriteArrayList<BaseNode>();
    private static EasyTaskConfig CONFIG = null;
    public static volatile boolean IS_STARTED = false;//是否已经启动
    /**
     * 当前节点是否属于首次启动注册到leader。默认是
     * 1、进程假死，或网络问题导致节点被leader踢出，然后要以新的节点加入集群时使用
     */
    public static volatile boolean IS_FIRST_STARTED = true;
    /**
     * 当前集群节点的Node对象
     */
    public static BaseNode CURRENT_NODE;

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
        return CONFIG;
    }


    public static void setConfig(EasyTaskConfig config) {
        CONFIG = config;
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
        CONFIG = config;
        DbInit.init();//初始化数据库
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        CmdServer.init();//启动命令服务的socket端口
        initCURRENT_NODE(true);//初始化本节点的集群服务
        IS_STARTED = true;
    }

    /**
     * 初始化当前节点的集群。
     * 1、系统重启或因心网络问题被leader踢出，然后又恢复了
     * 2、需要支持进程没死，然后重新初始化，可反复执行。以新的节点加入集群
     *
     * @param isFirstStarted 是否首次初始化。进程重启属于首次
     * @return
     */
    public static void initCURRENT_NODE(boolean isFirstStarted) throws Exception {
        clearThreadTask();
        if (isFirstStarted && !BrokerUtil.isAliveInCluster())
            BrokerUtil.clearAllData();
        CURRENT_NODE = new BaseNode(Util.getLocalIP(), CONFIG.getServerPort());
        MasterService.initMaster();
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
        timerTasks.add(MasterService.startMasterUpdateSlaveDataStatusTask());
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

    /**
     * 节点对leader的心跳。
     */
    public static TimerTask startHeartBeat() {
        HeartbeatsTask task = new HeartbeatsTask();
        task.start();
        return task;
    }

    /**
     * 启动点定时从leader获取注册表更新任务
     */
    public static TimerTask startUpdateRegeditTask() {
        BrokerRequestUpdateRegeditTask task = new BrokerRequestUpdateRegeditTask();
        task.start();
        return task;
    }

    /**
     * 启动点定时从leader更新Client列表。
     */
    public static TimerTask startBrokerUpdateClientsTask() {
        BrokerUpdateClientsTask task = new BrokerUpdateClientsTask();
        task.start();
        return task;
    }

    /**
     * 启动Broker通知客户端提交的任务同步结果反馈
     */
    public static TimerTask startBrokerNotifyClientSubmitTaskResultTask() {
        BrokerNotifyClientSubmitTaskResultTask task = new BrokerNotifyClientSubmitTaskResultTask();
        task.start();
        return task;
    }

    /**
     * Broker定时从leader获取注册表最新信息
     * 覆盖本地信息
     *
     * @return
     */
    public static boolean requestUpdateRegedit() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowRequestLeaderSendRegedit).setSource(CONFIG.getAddress())
                    .setBody(StringConstant.BROKER);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, CLUSTER_LEADER.getClient(), CONFIG.getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                NodeDto.Node node = NodeDto.Node.parseFrom(respPack.getRespbody());
                dealUpdate(node);
                return true;
            } else {
                LogUtil.info("normally exception!requestUpdateRegedit() failed.");
            }
        } catch (Exception e) {
            LogUtil.error("", e);
        }
        return false;
    }

    /**
     * 处理leader推送过来的注册表更新。
     * 1、leader重新选举了master或slave时，leader主动通知broker。
     * 2、broker会定时从leader请求最新的注册表信息同步到本地。防止期间数据不一致性问题，做到最终一致性
     * 3、更新备用leader、最新master集合、最新slave集合
     *
     * @param node
     */
    public static void dealUpdate(NodeDto.Node node) {
        BAK_LEADER = node.getBakleader();
        //处理slaves
        NodeDto.NodeList slaveNodes = node.getSalves();
        List<String> slavesAddress = new ArrayList<>(slaveNodes.getNodesList().size());
        slaveNodes.getNodesList().forEach(x -> {
            slavesAddress.add(new SlaveNode(x.getHost(), x.getPort()).getAddress());
        });
        //获取新加入的slave节点
        slavesAddress.forEach(x -> {
            if (!SlaveService.MASTERS.keySet().contains(x)) {
                MasterService.SLAVES.put(x, new SlaveNode(x));
            }
        });
        //删除已经失效的slave
        MasterService.SLAVES.keySet().forEach(x -> {
            if (!slavesAddress.contains(x)) {
                MasterService.SLAVES.remove(x);
            }
        });
        //处理masters
        NodeDto.NodeList masterNodes = node.getMasters();
        List<String> mastersAddress = new ArrayList<>(masterNodes.getNodesList().size());
        masterNodes.getNodesList().forEach(x -> {
            mastersAddress.add(new MasterNode(x.getHost(), x.getPort()).getAddress());
        });
        //获取新加入的master节点
        mastersAddress.forEach(x -> {
            if (!SlaveService.MASTERS.keySet().contains(x)) {
                SlaveService.MASTERS.put(x, new MasterNode(x));
            }
        });
        //删除已经失效的master
        SlaveService.MASTERS.keySet().forEach(x -> {
            if (!mastersAddress.contains(x)) {
                SlaveService.MASTERS.remove(x);
            }
        });
    }

}
