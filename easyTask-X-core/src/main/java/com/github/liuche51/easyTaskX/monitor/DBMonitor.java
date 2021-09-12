package com.github.liuche51.easyTaskX.monitor;

import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBMonitor {
    public static Map<String,List> getInfoByTaskId(String taskId) throws Exception {
        Map<String,List> map=new HashMap<>(3);
        List<TransactionLog> tranlogs= TranlogScheduleDao.selectByTaskId(taskId);
        List<Schedule> schedules= ScheduleDao.selectByTaskId(taskId);
        List<ScheduleSync> scheduleSyncs= ScheduleSyncDao.selectByTaskId(taskId);
        List<ScheduleBak> scheduleBaks= ScheduleBakDao.selectByTaskId(taskId);
        map.put(DbTableName.TRANLOG_SCHEDULE,tranlogs);
        map.put(DbTableName.SCHEDULE,schedules);
        map.put(DbTableName.SCHEDULE_SYNC,scheduleSyncs);
        map.put(DbTableName.SCHEDULE_BAK,scheduleBaks);
        return map;
    }
}
