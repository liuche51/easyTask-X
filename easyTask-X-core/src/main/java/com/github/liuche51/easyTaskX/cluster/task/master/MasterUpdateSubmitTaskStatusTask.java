package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;
import sun.security.krb5.internal.Ticket;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * master更新任务提交状态
 * 1、高可靠模式下使用
 */
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
                        MasterService.BINLOG_LAST_INDEX=ScheduleDao.updateByIds(ids.toArray(new String[]{}), values);
                        results.forEach(x->{ // 分别将处理成功的任务结果放入对应的客户端反馈队列中去。
                            MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT(x.getSource(),new SubmitTaskResult(x.getId(), SubmitTaskResultStatusEnum.SUCCESSED));
                        });

                    }
                } else {
                    try {
                        if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                            TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                        LogUtil.error("", e);
                    }
                }

            } catch (Exception e) {
                if (results.size() > 0) {//发生数据库访问异常，重新进队列以便重试。
                    results.forEach(x -> {
                        try {
                            MasterService.SLAVE_RESPONSE_SUCCESS_TASK_RESULT.put(x);//这里因为是异常情况下。所以直接用阻塞的api即可。
                        } catch (InterruptedException interruptedException) {
                            LogUtil.error("", e);
                        }
                    });
                }
                LogUtil.error("", e);
            }
        }
    }
}
