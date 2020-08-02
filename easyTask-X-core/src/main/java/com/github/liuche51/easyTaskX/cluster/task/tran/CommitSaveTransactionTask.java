package com.github.liuche51.easyTaskX.cluster.task.tran;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.TransactionLogDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 新增任务提交定时任务
 * 定时获取已经表记为CONFIRM的事务，并提交至数据表
 */
public class CommitSaveTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TransactionLog> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TransactionLog> scheduleList = null, scheduleBakList = null;
            List<Schedule> scheduleList1 =new LinkedList<>();
            List<ScheduleBak> scheduleBakList1 = new LinkedList<>();
            try {
                list = TransactionLogDao.selectByStatusAndType(TransactionStatusEnum.CONFIRM, TransactionTypeEnum.SAVE,100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTableName())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTableName())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    scheduleList.forEach(x->{
                        scheduleList1.add(JSONObject.parseObject(x.getContent(),Schedule.class));
                    });
                    ScheduleDao.saveBatch(scheduleList1);
                    String[] scheduleIds=scheduleList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    TransactionLogDao.updateStatusByIds(scheduleIds,TransactionStatusEnum.FINISHED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    scheduleBakList.forEach(x->{
                        scheduleBakList1.add(JSONObject.parseObject(x.getContent(),ScheduleBak.class));
                    });
                    ScheduleBakDao.saveBatch(scheduleBakList1);
                    String[] scheduleBakIds=scheduleBakList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    TransactionLogDao.updateStatusByIds(scheduleBakIds,TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("CommitSaveTransactionTask():exception!", e);
            }
            try {
                if (new Date().getTime()-getLastRunTime().getTime()<500)//防止频繁空转
                    Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
