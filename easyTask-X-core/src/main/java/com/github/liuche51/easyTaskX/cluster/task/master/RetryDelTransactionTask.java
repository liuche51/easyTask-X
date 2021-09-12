package com.github.liuche51.easyTaskX.cluster.task.master;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.master.DeleteTaskTCC;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
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
 * 任务删除事务重试第一阶段定时任务
 * 只处理事务已经标记为Try的。说明当前事务第一阶段就发生异常，需要重试
 * 通过最大努力通知的方式实现最终一致性
 */
public class RetryDelTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TranlogSchedule> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TranlogSchedule> scheduleList = null;
            try {
                list = TranlogScheduleDao.selectByStatusAndReTryCount(TransactionStatusEnum.TRIED, TransactionTypeEnum.DELETE, new Short("3"), 100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTableName())).collect(Collectors.toList());
                if (scheduleList != null && scheduleList.size() > 0) {
                    for (TranlogSchedule x : scheduleList) {
                        try {
                            //如果距离上次重试时间不足5分钟，则跳过重试
                            if (!StringUtils.isNullOrEmpty(x.getRetryTime())) {
                                System.out.println("x.getRetryTime()="+x.getRetryTime());
                                if (ZonedDateTime.now().minusMinutes(5)
                                        .compareTo(DateUtils.parse(x.getRetryTime())) > 0) {
                                    continue;
                                }
                            }
                            List<String> cancelSlavesHost = JSONObject.parseObject(x.getSlaves(), new TypeReference<List<String>>() {
                            });
                            List<BaseNode> cancelFollows = new ArrayList<>(cancelSlavesHost.size());
                            if (cancelSlavesHost != null) {
                                cancelSlavesHost.forEach(y -> {
                                    String[] hp = y.split(":");
                                    cancelFollows.add(new Node(hp[0], Integer.parseInt(hp[1])));
                                });
                                log.info("RetryDelTransactionTask()->tryDel():transactionId=" + x.getId() + " retryCount=" + x.getRetryCount() + ",retryTime=" + x.getRetryTime());
                                DeleteTaskTCC.retryDel(x.getId(), x.getContent(), cancelFollows);
                            }
                        } catch (Exception e) {
                            log.error("RetryDelTransactionTask item exception!", e);
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
