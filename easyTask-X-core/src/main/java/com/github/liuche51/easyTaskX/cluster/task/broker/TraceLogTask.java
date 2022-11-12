package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.TraceLogDao;
import com.github.liuche51.easyTaskX.dto.db.TraceLog;
import com.github.liuche51.easyTaskX.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 任务跟踪日志入库
 */
public class TraceLogTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                List<TraceLog> traceLogs=new ArrayList<>(1000);
                TraceLogUtil.TASK_TRACE_WAITTO_WRITEDB.drainTo(traceLogs,1000);
                TraceLogDao.saveBatch(traceLogs);

            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                TimeUnit.MILLISECONDS.sleep(BrokerService.getConfig().getAdvanceConfig().getTraceLogWriteIntervalTimes());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }
}
