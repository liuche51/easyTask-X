package com.github.liuche51.easyTaskX.cluster.task.slave;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;

/**
 * bakLeader定时从leader获取注册表更新
 */
public class BakLeaderRequestUpdateRegeditTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                String leader = NodeService.CURRENTNODE.getClusterLeader().getAddress();
                BaseNode node = NodeService.CURRENTNODE.getMasters().get(leader);
                //如果当前的leader同时也是自己的master，则需要定时同步注册表信息
                if (node != null)
                    SlaveService.requestUpdateClusterRegedit();
            } catch (Exception e) {
                log.error("BakLeaderRequestUpdateRegeditTask()->exception!", e);
            }
            try {
                Thread.sleep(NodeService.getConfig().getAdvanceConfig().getSlaveUpdateRegeditTime());
            } catch (InterruptedException e) {
                log.error("BakLeaderRequestUpdateRegeditTask()->exception!", e);
            }
        }
    }
}
