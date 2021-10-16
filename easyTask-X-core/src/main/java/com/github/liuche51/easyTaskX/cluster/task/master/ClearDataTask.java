package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;

import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.util.DateUtils;

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
                Map<String, BaseNode> leaders = NodeService.CURRENTNODE.getMasters();
                Iterator<Map.Entry<String, BaseNode>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                List<String> sources = new ArrayList<>(leaders.size());
                while (items.hasNext()) {
                    Map.Entry<String, BaseNode> item = items.next();
                    sources.add(item.getValue().getAddress());
                }
                ScheduleBakDao.deleteBySources(sources.toArray(new String[sources.size()]));
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
}
