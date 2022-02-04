package com.github.liuche51.easyTaskX.cluster.slave;

import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.ScheduleBinLogSyncTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.SlaveNotifyMasterHasSyncUnUseTaskTask;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Slave服务入口
 */
public class SlaveService {
    /**
     * 当前节点的所有masters
     */
    public static ConcurrentHashMap<String, MasterNode> MASTERS = new ConcurrentHashMap<>();
    /**
     * 等待salve反馈给master的任务状态
     * 1、高可靠模式下使用
     * 2、每个master都有单独的反馈队列
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<SubmitTaskResult>> WAIT_RESPONSE_MASTER_TASK_RESULT = new ConcurrentHashMap<>();

    /**
     * 启动从master获取ScheduleBinLog订阅任务。
     */
    public static TimerTask startScheduleBinLogSyncTask() {
        ScheduleBinLogSyncTask task = new ScheduleBinLogSyncTask();
        task.start();
        return task;
    }
    /**
     * 启动Slave通知Master提交的任务同步结果反馈任务。
     */
    public static TimerTask startSlaveNotifyMasterSubmitTaskResultTask() {
        SlaveNotifyMasterHasSyncUnUseTaskTask task = new SlaveNotifyMasterHasSyncUnUseTaskTask();
        task.start();
        return task;
    }
}
