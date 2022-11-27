package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.TraceLogDao;
import com.github.liuche51.easyTaskX.dto.db.TraceLog;
import com.github.liuche51.easyTaskX.enume.TaskTraceStoreModel;
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
                String taskTraceStoreModel = BrokerService.getConfig().getAdvanceConfig().getTaskTraceStoreModel();
                List<TraceLog> traceLogs=new ArrayList<>(1000);
                TraceLogUtil.TASK_TRACE_WAITTO_WRITE.drainTo(traceLogs,1000);
                if(TaskTraceStoreModel.LOCAL.equalsIgnoreCase(taskTraceStoreModel)){
                    TraceLogDao.saveBatch(traceLogs);
                }else if(TaskTraceStoreModel.EXT.equalsIgnoreCase(taskTraceStoreModel)){
                    String taskTraceExtUrl = BrokerService.getConfig().getAdvanceConfig().getTaskTraceExtUrl();
                    HttpRequest.sendPost(taskTraceExtUrl,"logs="+ JSONObject.toJSONString(traceLogs));
                }
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
