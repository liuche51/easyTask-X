package com.github.liuche51.easyTaskX.cluster.follow;

import com.github.liuche51.easyTaskX.cluster.task.HeartbeatsTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.UpdateRegeditTask;
import com.github.liuche51.easyTaskX.cluster.task.tran.*;

/**
 * Broker
 */
public class BrokerService {
    /**
     * 启动批量事务数据提交任务
     */
    public static TimerTask startCommitSaveTransactionTask() {
        CommitSaveTransactionTask task=new CommitSaveTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动批量事务数据删除任务
     */
    public static TimerTask startCommitDelTransactionTask() {
        CommitDelTransactionTask task=new CommitDelTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动批量事务数据取消提交任务
     */
    public static TimerTask startCancelSaveTransactionTask() {
        CancelSaveTransactionTask task=new CancelSaveTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动重试取消保持任务
     */
    public static TimerTask startRetryCancelSaveTransactionTask() {
        RetryCancelSaveTransactionTask task=new RetryCancelSaveTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动重试删除任务
     */
    public static TimerTask startRetryDelTransactionTask() {
        RetryDelTransactionTask task=new RetryDelTransactionTask();
        task.start();
        return task;
    }
    /**
     * 节点对集群leader的心跳。2s一次
     */
    public static TimerTask startHeartBeat() {
        HeartbeatsTask task=new HeartbeatsTask();
        task.start();
        return task;
    }
    /**
     * 启动点定时从集群leader获取注册表更新任务
     */
    public static TimerTask startUpdateRegeditTask() {
        UpdateRegeditTask task=new UpdateRegeditTask();
        task.start();
        return task;
    }
}
