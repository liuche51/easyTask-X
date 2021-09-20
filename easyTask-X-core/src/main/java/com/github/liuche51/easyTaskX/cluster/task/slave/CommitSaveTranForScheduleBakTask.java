package com.github.liuche51.easyTaskX.cluster.task.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.TranlogScheduleBak;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 新增备份任务提交定时任务
 * 定时获取已经表记为CONFIRM的事务，并提交至数据表
 */
public class CommitSaveTranForScheduleBakTask extends TimerTask {
    @Override
    public void run() {
        List<TranlogScheduleBak> tranlogScheduleBakList = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<ScheduleBak> scheduleBakList = new LinkedList<>();
            try {
                tranlogScheduleBakList = TranlogScheduleBakDao.selectByStatusAndType(TransactionStatusEnum.CONFIRM, TransactionTypeEnum.SAVE, 100);
                if (tranlogScheduleBakList != null && tranlogScheduleBakList.size() > 0) {
                    tranlogScheduleBakList.forEach(x -> {
                        scheduleBakList.add(JSONObject.parseObject(x.getContent(), ScheduleBak.class));
                    });
                    SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, ScheduleDao.getLock());
                    helper.beginTran();
                    try {
                        ScheduleBakDao.saveBatch(scheduleBakList);
                        String[] scheduleBakIds = tranlogScheduleBakList.stream().map(TranlogScheduleBak::getId).toArray(String[]::new);
                        TranlogScheduleBakDao.updateStatusByIds(scheduleBakIds, TransactionStatusEnum.FINISHED,helper);
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
