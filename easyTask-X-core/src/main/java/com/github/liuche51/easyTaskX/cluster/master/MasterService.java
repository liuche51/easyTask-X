package com.github.liuche51.easyTaskX.cluster.master;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.master.MasterDeleteTaskTask;
import com.github.liuche51.easyTaskX.cluster.task.master.MasterSubmitTask;
import com.github.liuche51.easyTaskX.cluster.task.master.MasterUpdateSubmitTaskStatusTask;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.util.LogErrorUtil;
import com.github.liuche51.easyTaskX.util.LogUtil;

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
    /**
     * 当前节点的所有slaves
     */
    public static ConcurrentHashMap<String, BaseNode> SLAVES = new ConcurrentHashMap<String, BaseNode>();
    /**
     * 等待入库同步的提交任务队列
     * 1、客户端提交任务到服务端，保存到此队列后，立即返回
     * 2、服务端异步通知任务最终是否提交成功
     */
    public static LinkedBlockingQueue<Schedule> WAIT_SUBMIT_TASK = new LinkedBlockingQueue<>(BrokerService.getConfig().getAdvanceConfig().getTaskQueueCapacity());
    /**
     * 等待删除的任务队列
     * 1、客户端或服务端内部提交删除任务到服务端，保存到此队列后，立即返回
     * 2、服务端异步通知任务最终是否提交成功
     */
    public static LinkedBlockingQueue<String> WAIT_DELETE_TASK = new LinkedBlockingQueue<>(BrokerService.getConfig().getAdvanceConfig().getTaskQueueCapacity());
    /**
     * 等待服务端反馈给客户端的任务状态
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<SubmitTaskResult>> WAIT_RESPONSE_CLINET_TASK_RESULT = new ConcurrentHashMap<>();
    /**
     * 高可靠模式下，Slave反馈给Master任务同步结果
     */
    public static LinkedBlockingQueue<SubmitTaskResult> SLAVE_RESPONSE_SUCCESS_TASK_RESULT = new LinkedBlockingQueue<>(BrokerService.getConfig().getAdvanceConfig().getTaskQueueCapacity());
    /**
     * master和slave同步提交任务状态记录
     * 1、高可靠模式下使用
     * 2、key=任务ID，value=任务来源地址,时间
     * 3、需要一个线程定时清理因为异常情况导致的长期未移除的数据。避免内存溢出
     */
    public static ConcurrentHashMap<String, Map<String, Object>> SLAVE_SYNC_TASK_RECORD = new ConcurrentHashMap<>();
    /**
     * 当前master节点binlog自增长最大位置编号
     * 1、用于判断其slave是否已经跟上数据同步
     */
    public static Long BINLOG_LAST_INDEX;

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
        List<BinlogSchedule> binlogSchedules = BinlogScheduleDao.getScheduleBinlogByIndex(index, BrokerService.getConfig().getAdvanceConfig().getBinlogCount());
        return binlogSchedules;
    }

    /**
     * 往所有broker发送队列里添加任务
     *
     * @param address
     */
    public static void addWAIT_RESPONSE_CLINET_TASK_RESULT(String address, SubmitTaskResult result) {
        LinkedBlockingQueue<SubmitTaskResult> queue = WAIT_RESPONSE_CLINET_TASK_RESULT.get(address);
        if (queue == null) {// 防止数据不一致导致未能正确添加Clinet的队列
            WAIT_RESPONSE_CLINET_TASK_RESULT.put(address, new LinkedBlockingQueue<SubmitTaskResult>(BrokerService.getConfig().getAdvanceConfig().getTaskQueueCapacity()));
            queue = WAIT_RESPONSE_CLINET_TASK_RESULT.get(address);
        }
        try {
            boolean offer = queue.offer(result, BrokerService.getConfig().getAdvanceConfig().getTimeOut(), TimeUnit.SECONDS);//插入队列，队列满时，超时抛出异常，以便能检查到原因
            if (offer == false) {
                LogErrorUtil.writeQueueErrorMsgToDb("队列WAIT_RESPONSE_CLINET_TASK_RESULT已满.", "com.github.liuche51.easyTaskX.cluster.master.MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT");
                addWAIT_DELETE_TASK(result.getId());
            }
        } catch (InterruptedException e) {
            LogUtil.error("", e);
        }

    }

    /**
     * 添加等待删除的任务
     *
     * @param taskId
     */
    public static void addWAIT_DELETE_TASK(String taskId) {
        try {
            boolean offer = MasterService.WAIT_DELETE_TASK.offer(taskId, BrokerService.getConfig().getAdvanceConfig().getTimeOut(), TimeUnit.SECONDS);//插入队列，队列满时，超时抛出异常，以便能检查到原因
            if (offer == false) {
                LogErrorUtil.writeQueueErrorMsgToDb("队列MasterService.WAIT_DELETE_TASK已满.", "com.github.liuche51.easyTaskX.cluster.master.MasterService.addWAIT_DELETE_TASK");
            }
        } catch (InterruptedException e) {
            LogUtil.error("", e);
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

    /**
     * 启动删除任务的定时任务
     */
    public static TimerTask startMasterDeleteTaskTask() {
        MasterDeleteTaskTask task = new MasterDeleteTaskTask();
        task.start();
        return task;
    }
}
