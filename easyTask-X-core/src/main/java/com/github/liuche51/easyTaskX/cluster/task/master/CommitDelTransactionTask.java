package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 任务删除事务批量提交定时任务
 * 只处理事务已经表记为删除确认的。如果master自己被标记为确认了，那么就可以认为其salve也已经被标记成确认提交事务了
 */
public class CommitDelTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TranlogSchedule> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TranlogSchedule> scheduleList = null, scheduleBakList = null;
            try {
                list = TranlogScheduleDao.selectByStatusAndType(new short[]{TransactionStatusEnum.CONFIRM,TransactionStatusEnum.TRIED}, TransactionTypeEnum.DELETE,100);
                //对于master来说，只能处理被标记为CONFIRM的事务。TRIED表示还需要重试通知salve标记删除TRIED状态
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTableName())&&TransactionStatusEnum.CONFIRM==x.getStatus()).collect(Collectors.toList());
                //对于salve来说。只需要事务被标记为TRIED状态，就可以执行删除操作了
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTableName())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    String[] scheduleIds=scheduleList.stream().map(TranlogSchedule::getContent).toArray(String[]::new);
                    ScheduleDao.deleteByIds(scheduleIds);
                    String[] scheduleTranIds=scheduleList.stream().map(TranlogSchedule::getId).toArray(String[]::new);
                    TranlogScheduleDao.updateStatusByIds(scheduleTranIds,TransactionStatusEnum.FINISHED);
                    ScheduleSyncDao.updateStatusByTransactionIds(scheduleTranIds, ScheduleSyncStatusEnum.DELETED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    String[] scheduleBakIds=scheduleBakList.stream().map(TranlogSchedule::getContent).toArray(String[]::new);
                    ScheduleBakDao.deleteByIds(scheduleBakIds);
                    String[] scheduleBakTranIds=scheduleBakList.stream().map(TranlogSchedule::getId).toArray(String[]::new);
                    TranlogScheduleDao.updateStatusByIds(scheduleBakTranIds,TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("", e);
            }
            try {
                if (new Date().getTime()-getLastRunTime().getTime()<500)//防止频繁空转
                    TimeUnit.MILLISECONDS.sleep(500L);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
