package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;

import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.util.DateUtils;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Master清理无用的数据定时任务
 */
public class ClearDataTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                clearScheduleBakData();
                clearSLAVE_SYNC_TASK_RECORD();
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                TimeUnit.HOURS.sleep(NodeService.getConfig().getAdvanceConfig().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    /**
     * 清理任务备份库数据
     * 1、master已经失效了的情况下。且自身不是新Master。如果是，则需要等到提交到自身主任务库后，才能清理。不再此处操作
     */
    private void clearScheduleBakData(){
        try {
            Map<String, BaseNode> leaders = NodeService.CURRENT_NODE.getMasters();
            Iterator<Map.Entry<String, BaseNode>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
            List<String> sources = new ArrayList<>(leaders.size());
            while (items.hasNext()) {
                Map.Entry<String, BaseNode> item = items.next();
                sources.add(item.getValue().getAddress());
            }
            ScheduleBakDao.deleteBySources(sources.toArray(new String[sources.size()]));
        }catch (Exception e){
            log.error("",e);
        }
    }

    /**
     * 清理Master的master和slave同步提交任务状态记录超时数据
     * 1、防止日积月累。导致内存问题
     */
    private void clearSLAVE_SYNC_TASK_RECORD() {
        Iterator<Map.Entry<String, Map<String, Object>>> items = MasterService.SLAVE_SYNC_TASK_RECORD.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Map<String, Object>> item = items.next();
            long time = (long) item.getValue().get("time");
            if (DateUtils.getTimeStamp(ZonedDateTime.now().plusHours(1)) > time) {
                items.remove();
                BrokerService.deleteTask(item.getKey());
            }
        }
    }
}
