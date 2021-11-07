package com.github.liuche51.easyTaskX.cluster.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.master.MasterSubmitTask;
import com.github.liuche51.easyTaskX.cluster.task.master.MasterUpdateSubmitTaskStatusTask;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.LogErrorUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * master服务入口
 */
public class MasterService {
    private static final Logger log = LoggerFactory.getLogger(MasterService.class);
    /**
     * 等待入库同步的提交任务队列
     * 1、客户端提交任务到服务端，保存到此队列后，立即返回
     * 2、服务端异步通知任务最终是否提交成功
     */
    public static LinkedBlockingQueue<Schedule> WAIT_SUBMIT_TASK = new LinkedBlockingQueue<>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity());
    /**
     * 等待服务端反馈给客户端的任务状态
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<SubmitTaskResult>> WAIT_RESPONSE_CLINET_TASK_RESULT = new ConcurrentHashMap<>();
    /**
     * 高可靠模式下，Slave反馈给Master任务同步结果
     */
    public static LinkedBlockingQueue<SubmitTaskResult> SLAVE_RESPONSE_SUCCESS_TASK_RESULT = new LinkedBlockingQueue<>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity());
    /**
     * master和slave同步提交任务状态记录
     * 1、高可靠模式下使用
     * 2、key=任务ID，value=任务来源地址,时间
     * 3、需要一个线程定时清理因为异常情况导致的长期未移除的数据。避免内存溢出
     */
    public static ConcurrentHashMap<String, Map<String, Object>> SLAVE_SYNC_TASK_RECORD = new ConcurrentHashMap<>();

    /**
     * 新Master将失效的旧Master的备份任务数据删除掉
     *
     * @param oldMasterAddress
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void deleteOldMasterBackTask(String oldMasterAddress) throws SQLException, ClassNotFoundException {
        ScheduleBakDao.deleteBySource(oldMasterAddress);
    }

    /**
     * 查询指定数据的binlog数据
     *
     * @param index
     * @return
     * @throws SQLException
     */
    public static List<BinlogSchedule> getScheduleBinlogByIndex(long index) throws SQLException {
        List<BinlogSchedule> binlogSchedules = BinlogScheduleDao.getScheduleBinlogByIndex(index, NodeService.getConfig().getAdvanceConfig().getBinlogCount());
        return binlogSchedules;
    }

    /**
     * master通知leader，变更slave与master数据同步状态
     *
     * @param dataStatus 1已完成同步。0同步中
     */
    public static void notifyNotifyLeaderChangeDataStatus(String salve, String dataStatus) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifyLeaderChangeSlaveDataStatus).setSource(NodeService.getConfig().getAddress())
                    .setBody(salve + StringConstant.CHAR_SPRIT_STRING + dataStatus);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENT_NODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (!ret) {
                LogErrorUtil.writeRpcErrorMsgToDb("master通知leader变更salve与master数据同步状态。失败！", "com.github.liuche51.easyTaskX.cluster.master.MasterService.notifyNotifyLeaderChangeDataStatus");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 往所有broker发送队列里添加任务
     *
     * @param address
     */
    public static void addWAIT_RESPONSE_CLINET_TASK_RESULT(String address, SubmitTaskResult result) {
        LinkedBlockingQueue<SubmitTaskResult> queue = WAIT_RESPONSE_CLINET_TASK_RESULT.get(address);
        if (queue == null) {// 防止数据不一致导致未能正确添加Clinet的队列
            WAIT_RESPONSE_CLINET_TASK_RESULT.put(address, new LinkedBlockingQueue<SubmitTaskResult>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity()));
            queue = WAIT_RESPONSE_CLINET_TASK_RESULT.get(address);
        }
        try {
            boolean offer = queue.offer(result, NodeService.getConfig().getAdvanceConfig().getTimeOut(), TimeUnit.SECONDS);//插入队列，队列满时，超时抛出异常，以便能检查到原因
            if (offer == false) {
                LogErrorUtil.writeQueueErrorMsgToDb("队列WAIT_RESPONSE_CLINET_TASK_RESULT已满.", "com.github.liuche51.easyTaskX.cluster.master.MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT");
                BrokerService.deleteTask(result.getId());
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }

    }

    /**
     * 启动点定时从leader获取注册表更新任务
     */
    public static TimerTask startMasterSubmitTask() {
        MasterSubmitTask task = new MasterSubmitTask();
        task.start();
        return task;
    }
    /**
     * 启动master更新任务提交状态任务
     */
    public static TimerTask startMasterUpdateSubmitTaskStatusTask() {
        MasterUpdateSubmitTaskStatusTask task = new MasterUpdateSubmitTaskStatusTask();
        task.start();
        return task;
    }
}
