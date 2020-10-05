package com.github.liuche51.easyTaskX.cluster;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.DeleteTaskTCC;
import com.github.liuche51.easyTaskX.cluster.master.SaveTaskTCC;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSlave;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.socket.CmdServer;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.util.exception.VotingException;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.zk.ZKService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NodeService {
    private static Logger log = LoggerFactory.getLogger(NodeService.class);
    private static EasyTaskConfig config = null;
    private static volatile boolean isStarted = false;//是否已经启动
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

    /**
     * 启动节点。
     * 线程互斥
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
        isStarted=true;
    }
    /**
     * 初始化当前节点的集群。(系统重启或心跳超时重启)
     * zk注册，选follows,开始心跳
     * 这里不考虑短暂宕机重启继续使用原follows情况。让原follows等待超时后重新选举leader就好了
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
        timerTasks.add(SlaveService.startClusterSlaveRequestUpdateRegeditTask());
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
        while (!isStarted|| VoteSlave.isSelecting()) {
            Thread.sleep(1000l);
        }
        this.submitTask(schedule);
    }
    /**
     * 任务数据持久化。并同步至备份库
     *使用TCC机制实现事务，达到数据最终一致性
     * @throws Exception
     */
    public static void submitTask(Schedule schedule) throws Exception {
        if (!isStarted) throw new Exception("the easyTask has not started,please wait a moment!");
        if (VoteSlave.isSelecting())
            throw new VotingException("normally exception!save():cluster is voting,please wait a moment.");
        //防止多线程下，follow元素操作竞争问题。确保参与提交的follow不受集群选举影响
        List<BaseNode> follows = new ArrayList<>(CURRENTNODE.getSlaves().size());
        Iterator<Map.Entry<String, BaseNode>> items = CURRENTNODE.getSlaves().entrySet().iterator();
        while (items.hasNext()) {
            follows.add(items.next().getValue());
        }
        if (follows.size() != NodeService.getConfig().getBackupCount())
            throw new Exception("save() exception！follows.size()!=backupCount");
        String transactionId=Util.generateTransactionId();
        try {
            SaveTaskTCC.trySave(transactionId,schedule, follows);
            SaveTaskTCC.confirm(transactionId, schedule.getId(), follows);
        } catch (Exception e) {
            log.error("saveTask():",e);
            try {
                SaveTaskTCC.cancel(transactionId, follows);
            }catch (Exception e1){
                log.error("saveTask()->cancel():",e);
                TransactionLogDao.updateRetryInfoById(transactionId, new Short("1"), DateUtils.getCurrentDateTime());
            }
            throw new Exception("task submit failed!");
        }
    }

    /**
     * 删除任务。含同步至备份库
     * 使用最大努力通知机制实现事务，达到数据最终一致性
     * 由于删除操作不需要回滚，不需要执行完整的TCC操作。必须要执行第一阶段即可
     * @param taskId
     * @return
     */
    public static boolean deleteTask(String taskId) {
        //防止多线程下，follow元素操作竞争问题。确保参与提交的follow不受集群选举影响
        List<BaseNode> follows = new ArrayList<>(CURRENTNODE.getSlaves().size());
        Iterator<Map.Entry<String,BaseNode>> items = CURRENTNODE.getSlaves().entrySet().iterator();
        while (items.hasNext()) {
            follows.add(items.next().getValue());
        }
        String transactionId=Util.generateTransactionId();
        try {
            DeleteTaskTCC.tryDel(transactionId,taskId, follows);
            return true;
        } catch (Exception e) {
            //如果写本地删除日志都失败了，那么就认为删除失败
            log.error("deleteTask exception!", e);
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
            TransactionLogDao.deleteByTypes(new short[]{TransactionTypeEnum.SAVE,TransactionTypeEnum.UPDATE});
        } catch (Exception e) {
            log.error("deleteAllData exception!", e);
        }
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