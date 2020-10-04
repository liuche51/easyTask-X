package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.Node;

import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TransactionLogDao;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.util.DateUtils;

import java.util.*;

/**
 * 清理无用的数据定时任务
 */
public class ClearDataTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> leaders = NodeService.CURRENTNODE.getMasters();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                List<String> sources = new ArrayList<>(leaders.size());
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    sources.add(item.getValue().getAddress());
                }
                ScheduleBakDao.deleteBySources(sources.toArray(new String[sources.size()]));
                TransactionLogDao.deleteByStatus(TransactionStatusEnum.FINISHED);
                ScheduleSyncDao.deleteByStatus(ScheduleSyncStatusEnum.DELETED);
                List<String> deleteids = new LinkedList<>();
                List<TransactionLog> tranlogList = TransactionLogDao.selectByStatus(TransactionStatusEnum.TRIED);
                tranlogList.forEach(x -> {
                    if (DateUtils.isGreaterThanSomeTime(DateUtils.parse(x.getCreateTime()), 300)) ;
                    deleteids.add(x.getId());
                });
                TransactionLogDao.deleteByIds(deleteids.toArray(new String[]{}));
            } catch (Exception e) {
                log.error("clearScheduleBak()", e);
            }
            try {
                Thread.sleep(NodeService.getConfig().getAdvanceConfig().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                log.error("clearScheduleBak()", e);
            }
        }
    }
}
