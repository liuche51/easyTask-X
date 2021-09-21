package com.github.liuche51.easyTaskX.cluster.task.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 新增任务提交定时任务
 * 定时获取已经表记为CONFIRM的事务，并提交至数据表
 */
public class CommitSaveTranForScheduleTask extends TimerTask {
    @Override
    public void run() {
        List<TranlogSchedule> tranlogScheduleList = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<Schedule> scheduleList = new LinkedList<>();
            try {
                tranlogScheduleList = TranlogScheduleDao.selectByStatusAndType(TransactionStatusEnum.CONFIRM,  100);
                if (tranlogScheduleList != null && tranlogScheduleList.size() > 0) {
                    tranlogScheduleList.forEach(x -> {
                        scheduleList.add(JSONObject.parseObject(x.getContent(), Schedule.class));
                    });
                    SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, ScheduleDao.getLock());
                    helper.beginTran();
                    try {
                        ScheduleDao.saveBatch(scheduleList, helper);
                        String[] scheduleIds = tranlogScheduleList.stream().map(TranlogSchedule::getId).toArray(String[]::new);
                        TranlogScheduleDao.updateStatusByIds(scheduleIds, TransactionStatusEnum.FINISHED, helper);
                        helper.commitTran();
                    } finally {
                        helper.destroyed();
                    }

                }

            } catch (Exception e) {
                log.error("", e);
            }
            try {
                if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                    TimeUnit.MILLISECONDS.sleep(500L);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
