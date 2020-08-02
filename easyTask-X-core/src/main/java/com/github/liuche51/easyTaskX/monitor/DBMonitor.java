package com.github.liuche51.easyTaskX.monitor;

import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TransactionLogDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.util.StringConstant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBMonitor {
    public static Map<String,List> getInfoByTaskId(String taskId) throws Exception {
        Map<String,List> map=new HashMap<>(3);
        List<TransactionLog> tranlogs= TransactionLogDao.selectByTaskId(taskId);
        List<Schedule> schedules= ScheduleDao.selectByTaskId(taskId);
        List<ScheduleSync> scheduleSyncs= ScheduleSyncDao.selectByTaskId(taskId);
        List<ScheduleBak> scheduleBaks= ScheduleBakDao.selectByTaskId(taskId);
        map.put(StringConstant.TRANSACTION_LOG,tranlogs);
        map.put(StringConstant.SCHEDULE,schedules);
        map.put(StringConstant.SCHEDULE_SYNC,scheduleSyncs);
        map.put(StringConstant.SCHEDULE_BAK,scheduleBaks);
        return map;
    }
}
