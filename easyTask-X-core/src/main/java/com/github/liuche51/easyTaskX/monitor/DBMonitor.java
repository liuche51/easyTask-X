package com.github.liuche51.easyTaskX.monitor;

import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBMonitor {
    public static Map<String,List> getInfoByTaskId(String taskId) throws Exception {
        Map<String,List> map=new HashMap<>(3);
        List<Schedule> schedules= ScheduleDao.selectByTaskId(taskId);
        List<ScheduleBak> scheduleBaks= ScheduleBakDao.selectByTaskId(taskId);
        map.put(DbTableName.SCHEDULE,schedules);
        map.put(DbTableName.SCHEDULE_BAK,scheduleBaks);
        return map;
    }
}
