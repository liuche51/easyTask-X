package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 提交客户端任务至集群存储
 */
public class MasterSubmitTaskTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                List<Schedule> schedules = new ArrayList<>(10);
                for (int i = 0; i < 10; i++) {
                    schedules.add(MasterService.WAIT_SUBMIT_TASK.take());
                }
                List<Schedule> molde1list=new ArrayList<>(schedules.size());// 普通模式的任务，Master本地保存成功后，直接进入反馈队列
                for (Schedule schedule : schedules) {
                    if (schedule.getSubmit_model() == 1) { // 普通模式
                        schedule.setStatus(ScheduleStatusEnum.NORMAL);
                        molde1list.add(schedule);
                        return;
                    } else if (schedule.getSubmit_model() == 2) { // 高可靠模式，需等待一个Slave同步成功才算成功
                        schedule.setStatus(ScheduleStatusEnum.UNUSE);
                    }
                }
                ScheduleDao.saveBatch(schedules);
                molde1list.forEach(x->{
                    MasterService.WAIT_RESPONSE_TASK_RESULT.get(x.getSource());
                });

            } catch (Exception e) {
                log.error("", e);
            }
            try {
                TimeUnit.HOURS.sleep(NodeService.getConfig().getAdvanceConfig().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
