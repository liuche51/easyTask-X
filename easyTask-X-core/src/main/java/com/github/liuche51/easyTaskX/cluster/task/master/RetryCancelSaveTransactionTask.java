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
        List<TranlogSchedule> tranlogScheduleList = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                tranlogScheduleList = TranlogScheduleDao.selectByStatusAndReTryCount(TransactionStatusEnum.CANCEL, new Short("3"), 100);
                if (tranlogScheduleList != null && tranlogScheduleList.size() > 0) {
                    String[] scheduleTranIds = tranlogScheduleList.stream().map(TranlogSchedule::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleTranIds);//先清掉自己已经提交的事务
                    for (TranlogSchedule x : tranlogScheduleList) {
                        try {
                            //如果距离上次重试时间不足5分钟，则跳过重试
                            if (!StringUtils.isNullOrEmpty(x.getRetryTime())) {
                                if (ZonedDateTime.now().minusMinutes(5)
                                        .compareTo(DateUtils.parse(x.getRetryTime())) > 0) {
                                    continue;
                                }
                            }
                            BaseNode cancelSlave = new BaseNode(x.getSlaves());
                            log.info("RetryDelTransactionTask()->retryCancel():transactionId=" + x.getId() + " retryCount=" + x.getRetryCount() + ",retryTime=" + x.getRetryTime());
                            SaveTaskTCC.retryCancel(x.getId(), cancelSlave);
                            TranlogScheduleDao.updateStatusById(x.getId(), TransactionStatusEnum.FINISHED);
                        } catch (Exception e) {
                            log.error("RetryCancelSaveTransactionTask() item exception!", e);
                            TranlogScheduleDao.updateRetryInfoById(x.getId(), (short) (x.getRetryCount() + 1), DateUtils.getCurrentDateTime());
                        }
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
