package com.github.liuche51.easyTaskX.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSlave;
import com.github.liuche51.easyTaskX.cluster.master.SaveTaskTCC;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerUpdateClientsTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.ReDispatchToClientTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerRequestUpdateRegeditTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.HeartbeatsTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.master.*;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.util.exception.VotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Broker
 */
public class BrokerService {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    /**
     * 客户端提交任务。允许线程等待，直到easyTask组件启动完成
     *
     * @param schedule
     * @return
     * @throws Exception
     */
    public void submitTaskAllowWait(Schedule schedule) throws Exception {
        //集群未启动或正在选举salve中，则继续等待完成
        while (!NodeService.isStarted || VoteSlave.isSelecting()) {
            TimeUnit.SECONDS.sleep(1L);
        }
        this.submitTask(schedule);
    }

    /**
     * 任务数据持久化。并同步至备份库
     * 1、提交模式为0，则只需要master保存成功即可。
     * 2、模式为1，则使用TCC机制实现事务，只需要取第一个slave做事务同步即可。其他的slave使用binlog异步同步
     *
     * @throws Exception
     */
    public static void submitTask(Schedule schedule) throws Exception {
        if (!NodeService.isStarted)
            throw new Exception("normally exception!the easyTask has not started,please wait a moment!");
        if (VoteSlave.isSelecting())
            throw new VotingException("normally exception!leader is voting slave,please wait a moment.");
        Object[] slaves = NodeService.CURRENTNODE.getSlaves().values().toArray();
        String transactionId = Util.generateTransactionId();
        if (NodeService.getConfig().getAdvanceConfig().getSubmitSuccessModel() == 0) {
            TranlogSchedule transactionLog = new TranlogSchedule();
            transactionLog.setId(transactionId);
            transactionLog.setContent(JSONObject.toJSONString(schedule));
            transactionLog.setStatus(TransactionStatusEnum.CONFIRM);
            transactionLog.setSlaves(StringConstant.EMPTY);
            TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        } else {
            try {
                SaveTaskTCC.trySave(transactionId, schedule, (BaseNode) slaves[0]);
                SaveTaskTCC.confirm(transactionId, (BaseNode) slaves[0]);
            } catch (Exception e) {
                log.error("", e);
                try {
                    SaveTaskTCC.cancel(transactionId, schedule.getId());
                } catch (Exception e1) {
                    log.error("", e);
                    TranlogScheduleDao.updateRetryInfoById(transactionId, new Short("1"), DateUtils.getCurrentDateTime());
                }
                throw new Exception("task submit failed!");
            }
        }

    }

    /**
     * 删除任务。
     * 1、slaves通过异步复制binlog同步删除
     *
     * @param taskId
     * @return
     */
    public static boolean deleteTask(String taskId) {
        SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, ScheduleDao.getLock());
        try {
            helper.beginTran();
            ScheduleDao.deleteByIds(new String[]{taskId}, helper);
            helper.commitTran();
            return true;
        } catch (Exception e) {
            log.error("", e);
            return false;
        } finally {
            helper.destroyed();
        }
    }

    /**
     * 更新任务。含同步至备份库
     * 1、slaves通过异步复制binlog同步更新
     *
     * @param taskIds
     * @return
     */
    public static boolean updateTask(String[] taskIds, Map<String, String> values) {
        SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, ScheduleDao.getLock());
        try {
            Iterator<Map.Entry<String, String>> items2 = values.entrySet().iterator();
            //组装更新字段。目前只有一个字段。支持未来扩展成多个字段
            StringBuilder updatestr = new StringBuilder();
            while (items2.hasNext()) {
                Map.Entry<String, String> item2 = items2.next();
                switch (item2.getKey()) {
                    case "executer":
                        updatestr.append("executer='").append(item2.getValue()).append("'");
                        break;
                    default:
                        break;
                }
            }
            helper.beginTran();
            ScheduleDao.updateByIds(taskIds, updatestr.toString(), helper);
            helper.commitTran();
            return true;
        } catch (Exception e) {
            log.error("", e);
            return false;
        } finally {
            helper.destroyed();
        }
    }

    /**
     * 启动批量事务数据提交任务
     */
    public static TimerTask startCommitSaveTransactionTask() {
        CommitSaveTranForScheduleTask task = new CommitSaveTranForScheduleTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }

    /**
     * 节点对leader的心跳。
     */
    public static TimerTask startHeartBeat() {
        HeartbeatsTask task = new HeartbeatsTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }

    /**
     * 启动点定时从leader获取注册表更新任务
     */
    public static TimerTask startUpdateRegeditTask() {
        BrokerRequestUpdateRegeditTask task = new BrokerRequestUpdateRegeditTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }

    /**
     * 启动点定时从leader更新Client列表。
     */
    public static TimerTask startBrokerUpdateClientsTask() {
        BrokerUpdateClientsTask task = new BrokerUpdateClientsTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }

    /**
     * 启动Broker重新将旧Client任务分配给新Clientd的任务。
     * 需要保证幂等性。防止接口重复触发相同任务
     */
    public static synchronized OnceTask startReDispatchToClientTask(BaseNode oldClient) {
        ReDispatchToClientTask task = new ReDispatchToClientTask(oldClient);
        String key = task.getClass().getName() + "," + oldClient.getAddress();
        if (ReDispatchToClientTask.runningTask.contains(key)) return null;
        ReDispatchToClientTask.runningTask.put(key, null);
        task.start();
        NodeService.onceTasks.add(task);
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
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowRequestLeaderSendRegedit).setSource(NodeService.getConfig().getAddress())
                    .setBody(StringConstant.BROKER);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                NodeDto.Node node = NodeDto.Node.parseFrom(respPack.getRespbody());
                NodeService.CURRENTNODE.setBakLeader(node.getBakleader());
                dealUpdate(node);
                return true;
            } else {
                log.info("normally exception!requestUpdateRegedit() failed.");
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 处理注册表更新。
     * 1、leader重新选举了master或slave时，leader主动通知broker。
     * 2、broker会定时从leader请求最新的注册表信息同步到本地。防止期间数据不一致性问题，做到最终一致性
     *
     * @param node
     */
    public static void dealUpdate(NodeDto.Node node) {
        NodeDto.NodeList slaveNodes = node.getSalves();
        ConcurrentHashMap<String, BaseNode> slaves = new ConcurrentHashMap<>();
        slaveNodes.getNodesList().forEach(x -> {
            slaves.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeDto.NodeList masterNodes = node.getMasters();
        ConcurrentHashMap<String, BaseNode> masters = new ConcurrentHashMap<>();
        masterNodes.getNodesList().forEach(x -> {
            masters.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeService.CURRENTNODE.setSlaves(slaves);
        NodeService.CURRENTNODE.setMasters(masters);
        BrokerUtil.updateMasterBinlogInfo(masters);
    }

    /**
     * Broker通知Client接受执行新任务
     *
     * @param newClient
     * @param schedules
     * @return
     * @throws Exception
     */
    public static boolean notifyClientExecuteNewTask(BaseNode newClient, List<Schedule> schedules) throws Exception {
        ScheduleDto.ScheduleList.Builder builder0 = ScheduleDto.ScheduleList.newBuilder();
        for (Schedule schedule : schedules) {
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            builder0.addSchedules(s);
        }
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BrokerNotifyClientExecuteNewTask).setSource(NodeService.getConfig().getAddress())
                .setBodyBytes(builder0.build().toByteString());
        NettyClient client = newClient.getClientWithCount(1);
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
        return ret;
    }

    /**
     * broker通知leader，已经完成重新分配任务至新client以及salve的数据同步。请求更新数据同步状态
     */
    public static void notifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus(Short reDispatchTaskStatus) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setBody(NettyInterfaceEnum.BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(String.valueOf(reDispatchTaskStatus));
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus() failed.");
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }
}
