package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 删除任务的定时任务
 * 1、批量方式从删除队列中获取任务执行本地删除
 * 2、slaves通过binlog方式异步删除
 */
public class MasterDeleteTaskTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            List<String> taskIds = new ArrayList<>(10);
            try {
                MasterService.WAIT_DELETE_TASK.drainTo(taskIds, 10);// 批量获取，为空不阻塞。
                if (taskIds.size() > 0) {
                    ScheduleDao.deleteByIds(taskIds.toArray(new String[]{}));
                } else {
                    try {
                        if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                            TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                        LogUtil.error("", e);
                    }
                }

            } catch (Exception e) {
                if (taskIds.size() > 0) { // 遇到删除失败，则重新放入队列重试.防止偶发性错误
                    taskIds.forEach(x -> {
                        MasterService.addWAIT_DELETE_TASK(x);
                    });
                }
                LogUtil.error("", e);
            }
        }
    }
}
