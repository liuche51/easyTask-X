package com.github.liuche51.easyTaskX.cluster.task.master;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.master.SaveTaskTCC;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 重试取消任务提交定时任务
 * 获取事务标记为CANCEL状态的。重试多次取消任务提交
 */
public class RetryCancelSaveTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TranlogSchedule> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TranlogSchedule> scheduleList = null, scheduleBakList = null;
            try {
                list = TranlogScheduleDao.selectByStatusAndReTryCount(TransactionStatusEnum.CANCEL, TransactionTypeEnum.SAVE, new Short("3"), 100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTableName())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTableName())).collect(Collectors.toList());
                if (scheduleList != null && scheduleList.size() > 0) {
                    String[] scheduleTranIds = scheduleList.stream().map(TranlogSchedule::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleTranIds);//先清掉自己已经提交的事务
                    for (TranlogSchedule x : scheduleList) {
                        try {
                            //如果距离上次重试时间不足5分钟，则跳过重试
                            if (!StringUtils.isNullOrEmpty(x.getRetryTime())) {
                                if (ZonedDateTime.now().minusMinutes(5)
                                        .compareTo(DateUtils.parse(x.getRetryTime())) > 0) {
                                    continue;
                                }
                            }
                            List<String> cancelFollowsHost = JSONObject.parseObject(x.getSlaves(), new TypeReference<List<String>>() {});
                            List<BaseNode> cancelFollows = new ArrayList<>(cancelFollowsHost.size());
                            if (cancelFollowsHost != null) {
                                cancelFollowsHost.forEach(y -> {
                                    String[] hp = y.split(":");
                                    cancelFollows.add(new Node(hp[0], Integer.parseInt(hp[1])));
                                });
                                log.info("RetryDelTransactionTask()->retryCancel():transactionId="+x.getId()+" retryCount="+x.getRetryCount()+",retryTime="+x.getRetryTime());
                                SaveTaskTCC.retryCancel(x.getId(), cancelFollows);
                            }
                            TranlogScheduleDao.updateStatusById(x.getId(), TransactionStatusEnum.FINISHED);
                        } catch (Exception e) {
                            log.error("RetryCancelSaveTransactionTask() item exception!", e);
                            TranlogScheduleDao.updateRetryInfoById(x.getId(), (short) (x.getRetryCount() + 1), DateUtils.getCurrentDateTime());
                        }
                    }
                }
                if (scheduleBakList != null && scheduleBakList.size() > 0) {
                    String[] scheduleBakIds = scheduleList.stream().map(TranlogSchedule::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleBakIds);
                    TranlogScheduleDao.updateStatusByIds(scheduleBakIds, TransactionStatusEnum.FINISHED);
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
