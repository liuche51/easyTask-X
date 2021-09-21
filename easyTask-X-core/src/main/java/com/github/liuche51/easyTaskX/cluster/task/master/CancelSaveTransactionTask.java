package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 删除已提交的任务定时任务
 * 只处理事务已经标记为取消CANCEL状态的。
 */
public class CancelSaveTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TranlogSchedule> tranlogScheduleList = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                tranlogScheduleList = TranlogScheduleDao.selectByStatusAndType(TransactionStatusEnum.CANCEL, 100);
                if (tranlogScheduleList != null && tranlogScheduleList.size() > 0) {
                    String[] scheduleIds = tranlogScheduleList.stream().map(TranlogSchedule::getContent).toArray(String[]::new);
                    SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, ScheduleDao.getLock());
                    helper.beginTran();
                    try {
                        ScheduleDao.deleteByIds(scheduleIds);
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
