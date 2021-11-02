package com.github.liuche51.easyTaskX.cluster.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.ReDispatchToClientTask;
import com.github.liuche51.easyTaskX.cluster.task.master.MasterSubmitTask;
import com.github.liuche51.easyTaskX.cluster.task.master.NewMasterSyncBakDataTask;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
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
    public static LinkedBlockingQueue<Schedule> WAIT_SUBMIT_TASK=new LinkedBlockingQueue<>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity());
    /**
     * 等待服务端反馈给客户端的任务状态
     */
    public static ConcurrentHashMap<String,LinkedBlockingQueue<SubmitTaskResult>> WAIT_RESPONSE_TASK_RESULT=new ConcurrentHashMap<>();
    /**
     * 高可靠模式下，Slave反馈给Master任务同步结果状态
     */
    public static LinkedBlockingQueue<SubmitTaskResult> SLAVE_RESPONSE_TASK_STATUS=new LinkedBlockingQueue<>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity());

    /**
     * 任务同步到slave状态记录
     * 1、高可靠模式下使用。key=任务ID，value=是否已经有salve同步任务
     * 2、salve同步任务使用binlog方式同步
     */
    public static ConcurrentHashMap<String, Boolean> TASK_SYNC_SALVE_STATUS = new ConcurrentHashMap<>();

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
     * 新master将旧master的备份数据同步给自己的slave
     * 后期需要考虑数据一致性
     *
     * @param oldMasterAddress
     */
    public static synchronized OnceTask submitNewTaskByOldMaster(String oldMasterAddress) {
        NewMasterSyncBakDataTask task = new NewMasterSyncBakDataTask(oldMasterAddress);
        String key = task.getClass().getName() + "," + oldMasterAddress;
        if (ReDispatchToClientTask.runningTask.contains(key)) return null;
        ReDispatchToClientTask.runningTask.put(key, null);
        task.start();
        NodeService.onceTasks.add(task);
        return task;
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
                NettyMsgService.writeRpcErrorMsgToDb("master通知leader变更salve与master数据同步状态。失败！", "com.github.liuche51.easyTaskX.cluster.master.MasterService.notifyNotifyLeaderChangeDataStatus");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }
    /**
     * 往所有broker发送队列里添加任务
     *
     * @param schedule
     */
    public static void addWait_Response_Task_Result(String address,SubmitTaskResult result) throws Exception {
        LinkedBlockingQueue<SubmitTaskResult> queue = WAIT_RESPONSE_TASK_RESULT.get(address);
        if (queue == null) {// 防止数据不一致导致未能正确添加Clinet的队列
            WAIT_RESPONSE_TASK_RESULT.put(address, new LinkedBlockingQueue<SubmitTaskResult>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity()));
            queue=WAIT_RESPONSE_TASK_RESULT.get(address);
        }
        boolean offer = queue.offer(schedule, schedule.getSubmitTimeout(), TimeUnit.SECONDS);
        if (offer == false) {
            throw new Exception("Queue WAIT_SEND_TASK is full.Please wait a moment try agin.");
        } else {
            if (schedule.getSubmitModel() == 0) {
                return;
            } else {
                SubmitTaskResult lock = new SubmitTaskResult();
                TASK_SYNC_BROKER_STATUS.put(schedule.getId(), lock);
                synchronized (lock) {
                    lock.wait(schedule.getSubmitTimeout() * 1000);//等待服务端提交任务最终成功后唤醒
                    SubmitTaskResult submitTaskResult = TASK_SYNC_BROKER_STATUS.get(schedule.getId());
                    if (submitTaskResult != null) {
                        switch (submitTaskResult.getStatus()) {
                            case 0://如果线程被唤醒，判断下任务的状态。为0表示超时自动被唤醒的。
                                deleteTask(schedule.getId(), schedule.getSubmitBroker());//任务有可能后续可能提交成功.触发任务删除操作
                                throw new Exception("Task submit timeout,Please try agin.");
                            case 1://服务端已经反馈任务提交成功
                                return;
                            case 9:
                                throw new Exception("Task submit failed,Please try agin." + submitTaskResult.getError());
                            default:
                                break;
                        }
                        TASK_SYNC_BROKER_STATUS.remove(schedule.getId());
                    } else {
                        throw new Exception("Task submit failed,Please try agin.");
                    }
                }
            }
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
}
