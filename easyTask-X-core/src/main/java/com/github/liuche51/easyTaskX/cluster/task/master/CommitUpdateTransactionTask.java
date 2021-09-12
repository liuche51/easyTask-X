package com.github.liuche51.easyTaskX.cluster.task.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.util.StringConstant;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 任务更新事务批量提交定时任务
 * 只处理事务已经表记为更新确认的。如果master自己被标记为确认了，那么就可以认为其slave也已经被标记成确认提交事务了
 */
public class CommitUpdateTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TransactionLog> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TransactionLog> scheduleList = null, scheduleBakList = null;
            try {
                list = TranlogScheduleDao.selectByStatusAndType(new short[]{TransactionStatusEnum.CONFIRM,TransactionStatusEnum.TRIED}, TransactionTypeEnum.DELETE,100);
                //对于master来说，只能处理被标记为CONFIRM的事务。TRIED表示还需要重试通知slave标记更新TRIED状态
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTableName())&&TransactionStatusEnum.CONFIRM==x.getStatus()).collect(Collectors.toList());
                //对于slave来说。只需要事务被标记为TRIED状态，就可以执行更新操作了
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTableName())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    list.forEach(x->{
                        try {
                            String[] items=x.getContent().split(StringConstant.CHAR_SPRIT_STRING);//taskids+values
                            String[] ids=items[0].split(StringConstant.CHAR_SPRIT_COMMA);
                            Map<String,String> values= JSONObject.parseObject(items[1],Map.class);
                            Iterator<Map.Entry<String,String>> items2=values.entrySet().iterator();
                            //组装更新字段。目前只有一个字段。支持未来扩展成多个字段
                            StringBuilder updatestr=new StringBuilder();
                            while (items2.hasNext()){
                                Map.Entry<String,String> item2=items2.next();
                                switch (item2.getKey()){
                                    case "executer":
                                        updatestr.append("executer='").append(item2.getValue()).append("'");
                                        break;
                                    default:break;
                                }
                            }
                            ScheduleDao.updateByIds(ids,updatestr.toString());
                        }catch (Exception e){
                            log.error("", e);
                        }


                    });
                    String[] scheduleIds=scheduleList.stream().map(TransactionLog::getContent).toArray(String[]::new);
                    ScheduleDao.deleteByIds(scheduleIds);
                    String[] scheduleTranIds=scheduleList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    TranlogScheduleDao.updateStatusByIds(scheduleTranIds,TransactionStatusEnum.FINISHED);
                    ScheduleSyncDao.updateStatusByTransactionIds(scheduleTranIds, ScheduleSyncStatusEnum.DELETED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    String[] scheduleBakIds=scheduleBakList.stream().map(TransactionLog::getContent).toArray(String[]::new);
                    ScheduleBakDao.deleteByIds(scheduleBakIds);
                    String[] scheduleBakTranIds=scheduleBakList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    TranlogScheduleDao.updateStatusByIds(scheduleBakTranIds,TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("", e);
            }
            try {
                if (new Date().getTime()-getLastRunTime().getTime()<500)//防止频繁空转
                    TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
