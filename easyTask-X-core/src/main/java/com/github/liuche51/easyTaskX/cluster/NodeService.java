package com.github.liuche51.easyTaskX.cluster;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.DeleteTaskTCC;
import com.github.liuche51.easyTaskX.cluster.master.SaveTaskTCC;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSlave;
import com.github.liuche51.easyTaskX.cluster.master.UpdateTaskTCC;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.master.ClearDataTask;
import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.socket.CmdServer;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.util.exception.VotingException;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.zk.ZKService;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NodeService {
    private static Logger log = LoggerFactory.getLogger(NodeService.class);
    private static EasyTaskConfig config = null;
    private static volatile boolean isStarted = false;//是否已经启动
    public static volatile boolean isFirstStarted=true;//当前节点是否属于首次启动注册到leader。默认是
    /**
     * 集群所有可用的clients
     */
    private ConcurrentHashMap<String, BaseNode> clients = new ConcurrentHashMap<String, BaseNode>();
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENTNODE;
    /**
     * 集群一次性任务线程集合。
     * 系统没有重启只是初始化了集群initCurrentNode()。此时也需要立即停止运行的一次性后台任务
     * 需要定时检查其中的线程是否已经运行完，完了需要移除线程对象，释放内存资源
     */
    public static List<OnceTask> onceTasks = new LinkedList<OnceTask>();
    /**
     * 集群定时任务线程集合。
     * 系统没有重启只是初始化了集群initCurrentNode()。此时需要停止之前的定时任务，重新启动新的
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
        if (isStarted)
            return;
        if (config == null)
            throw new Exception("config is null,please set a EasyTaskConfig!");
        EasyTaskConfig.validateNecessary(config);
        NodeService.config = config;
        DbInit.init();//初始化数据库
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        CmdServer.init();//启动命令服务的socket端口
        initCurrentNode();//初始化本节点的集群服务
        isStarted = true;
    }

    /**
     * 初始化当前节点的集群。
     * (系统重启或因心网络问题被leader踢出，然后又恢复了)
     *
     * @return
     */
    public static void initCurrentNode() throws Exception {
        clearThreadTask();
        deleteAllData();
        CURRENTNODE = new Node(Util.getLocalIP(), NodeService.getConfig().getServerPort());
        timerTasks.add(BrokerService.startHeartBeat());
        timerTasks.add(clearDataTask());
        timerTasks.add(BrokerService.startCommitSaveTransactionTask());
        timerTasks.add(BrokerService.startCommitDelTransactionTask());
        timerTasks.add(BrokerService.startCancelSaveTransactionTask());
        timerTasks.add(BrokerService.startRetryCancelSaveTransactionTask());
        timerTasks.add(BrokerService.startRetryDelTransactionTask());
        timerTasks.add(BrokerService.startUpdateRegeditTask());
        timerTasks.add(SlaveService.startBakLeaderRequestUpdateRegeditTask());
        timerTasks.add(BrokerService.startBrokerUpdateClientsTask());
        ZKService.listenLeaderDataNode();
    }

    /**
     * 客户端提交任务。允许线程等待，直到easyTask组件启动完成
     *
     * @param schedule
     * @return
     * @throws Exception
     */
    public void submitTaskAllowWait(Schedule schedule) throws Exception {
        //集群未启动或正在选举follow中，则继续等待完成
        while (!isStarted || VoteSlave.isSelecting()) {
            TimeUnit.SECONDS.sleep(1L);
        }
        this.submitTask(schedule);
    }

    /**
     * 任务数据持久化。并同步至备份库
     * 使用TCC机制实现事务，达到数据最终一致性
     *
     * @throws Exception
     */
    public static void submitTask(Schedule schedule) throws Exception {
        if (!isStarted) throw new Exception("normally exception!the easyTask has not started,please wait a moment!");
        if (VoteSlave.isSelecting())
            throw new VotingException("normally exception!leader is voting slave,please wait a moment.");
        //防止多线程下，follow元素操作竞争问题。确保参与提交的follow不受集群选举影响
        List<BaseNode> slaves = new ArrayList<>(CURRENTNODE.getSlaves().size());
        Iterator<Map.Entry<String, BaseNode>> items = CURRENTNODE.getSlaves().entrySet().iterator();
        while (items.hasNext()) {
            slaves.add(items.next().getValue());
        }
        if (slaves.size() != NodeService.getConfig().getBackupCount())
            throw new Exception("slaves.size()!=backupCount");
        String transactionId = Util.generateTransactionId();
        try {
            SaveTaskTCC.trySave(transactionId, schedule, slaves);
            SaveTaskTCC.confirm(transactionId, schedule.getId(), slaves);
        } catch (Exception e) {
            log.error("", e);
            try {
                SaveTaskTCC.cancel(transactionId, slaves);
            } catch (Exception e1) {
                log.error("", e);
                TransactionLogDao.updateRetryInfoById(transactionId, new Short("1"), DateUtils.getCurrentDateTime());
            }
            throw new Exception("task submit failed!");
        }
    }

    /**
     * 删除任务。含同步至备份库
     * 使用最大努力通知机制实现事务，达到数据最终一致性
     * 由于删除操作不需要回滚，不需要执行完整的TCC操作。必须要执行第一阶段即可
     *
     * @param taskId
     * @return
     */
    public static boolean deleteTask(String taskId) {
        //防止多线程下，slave元素操作竞争问题。确保参与提交的slave不受集群选举影响
        List<BaseNode> slaves = new ArrayList<>(CURRENTNODE.getSlaves().size());
        Iterator<Map.Entry<String, BaseNode>> items = CURRENTNODE.getSlaves().entrySet().iterator();
        while (items.hasNext()) {
            slaves.add(items.next().getValue());
        }
        String transactionId = Util.generateTransactionId();
        try {
            DeleteTaskTCC.tryDel(transactionId, taskId, slaves);
            return true;
        } catch (Exception e) {
            //如果写本地删除日志都失败了，那么就认为删除失败
            log.error("", e);
            return false;
        }
    }

    /**
     * 更新任务。含同步至备份库
     * 使用最大努力通知机制实现事务，达到数据最终一致性
     * 由于更新操作不需要回滚，不需要执行完整的TCC操作。必须要执行第一阶段即可
     *
     * @param taskIds
     * @return
     */
    public static boolean updateTask(String[] taskIds, Map<String, String> values) {
        List<BaseNode> slaves = new ArrayList<>(CURRENTNODE.getSlaves().size());
        Iterator<Map.Entry<String, BaseNode>> items = CURRENTNODE.getSlaves().entrySet().iterator();
        while (items.hasNext()) {
            slaves.add(items.next().getValue());
        }
        String transactionId = Util.generateTransactionId();
        try {
            UpdateTaskTCC.tryUpdate(transactionId, taskIds, slaves, values);
            return true;
        } catch (Exception e) {
            //如果写本地更新日志都失败了，那么就认为删除失败
            log.error("", e);
            return false;
        }
    }

    /**
     * 清空所有表的记录
     * 节点宕机后，重启。或失去联系zk后又重新连接了。都视为新节点加入集群。加入前需要清空所有记录，避免有重复数据在集群中
     */
    public static void deleteAllData() {
        try {
            ScheduleDao.deleteAll();
            ScheduleBakDao.deleteAll();
            ScheduleSyncDao.deleteAll();
            //目前不清除删除类型的事务。因为这样系统重启后可以继续删除操作。最大限度保障从集群中删除
            TransactionLogDao.deleteByTypes(new short[]{TransactionTypeEnum.SAVE, TransactionTypeEnum.UPDATE});
        } catch (Exception e) {
            log.error("deleteAllData exception!", e);
        }
    }

    public static void changeTaskClient(String oldClinet) {

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
     * 测试目标主机是否都可以联通
     *
     * @param list
     * @return
     */
    public static boolean canAllConnect(List<BaseNode> list) {
        for (BaseNode node : list) {
            try {
                NettyClient client = node.getClient();
                if (client == null) return false;
            } catch (Exception e) {
                log.info("normally exception!canAllConnect() failed.object address=" + node.getAddress());
                return false;
            }
        }
        return true;
    }
}
