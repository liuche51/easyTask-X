package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 提交客户端任务至本地存储
 */
public class MasterSubmitTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            List<Schedule> schedules = new ArrayList<>(10);
            try {
                MasterService.WAIT_SUBMIT_TASK.drainTo(schedules, 10);// 批量获取，为空不阻塞。
                if (schedules.size() > 0) {
                    List<Schedule> molde1list = new ArrayList<>(schedules.size());// 普通模式的任务，Master本地保存成功后，直接进入反馈队列
                    Map<String, Map<String, Object>> model2list = new HashMap<>();// 高可靠模式的任务
                    for (Schedule schedule : schedules) {
                        if (schedule.getSubmit_model() == 1) { // 普通模式
                            schedule.setStatus(ScheduleStatusEnum.NORMAL);
                            molde1list.add(schedule);
                            return;
                        } else if (schedule.getSubmit_model() == 2) { // 高可靠模式，需等待一个Slave同步成功才算成功
                            schedule.setStatus(ScheduleStatusEnum.UNUSE);
                            Map<String, Object> item=new HashMap<>();
                            item.put("source",schedule.getSource());
                            item.put("time",System.currentTimeMillis());
                            model2list.put(schedule.getId(), item);
                        }
                    }
                    ScheduleDao.saveBatch(schedules);
                    MasterService.SLAVE_SYNC_TASK_RECORD.putAll(model2list);
                    molde1list.forEach(x -> {
                        MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT(x.getSource(), new SubmitTaskResult(x.getId(), SubmitTaskResultStatusEnum.SUCCESSED));
                    });
                } else {
                    try {
                        if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                            TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                        LogUtil.error("", e);
                    }
                }

            } catch (Exception e) {
                if (schedules.size() > 0) {
                    schedules.forEach(x -> {
                        MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT(x.getSource(), new SubmitTaskResult(x.getId(), SubmitTaskResultStatusEnum.FAILED, "Master 持久化任务异常!"));
                    });
                }
                LogUtil.error("", e);
            }
        }
    }
}
