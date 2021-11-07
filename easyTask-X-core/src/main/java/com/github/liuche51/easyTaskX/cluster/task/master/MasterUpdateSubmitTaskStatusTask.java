package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import sun.security.krb5.internal.Ticket;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MasterUpdateSubmitTaskStatusTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            List<SubmitTaskResult> results = new ArrayList<>(10);
            try {
                MasterService.SLAVE_RESPONSE_SUCCESS_TASK_RESULT.drainTo(results, 10);// 批量获取，为空不阻塞。
                if (results.size() > 0) {
                    List<String> ids = new ArrayList<>(results.size());// 处理成功的任务
                    results.forEach(x -> {
                            ids.add(x.getId());
                    });
                    if (ids.size() > 0) {
                        Map<String, Object> values = new HashMap<>();
                        values.put("status", ScheduleStatusEnum.NORMAL);
                        ScheduleDao.updateByIds(ids.toArray(new String[]{}), values);
                        results.forEach(x->{ // 分别将处理成功的任务结果放入对应的客户端反馈队列中去。
                            MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT(x.getSource(),new SubmitTaskResult(x.getId(),1));
                        });

                    }
                } else {
                    try {
                        if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                            TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                        log.error("", e);
                    }
                }

            } catch (Exception e) {
                if (schedules.size() > 0) {
                    schedules.forEach(x -> {
                        MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT(x.getSource(), new SubmitTaskResult(x.getId(), 9, "Master 持久化任务异常!"));
                    });
                }
                log.error("", e);
            }
        }
    }
}
