package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.Node;

/**
 * 集群Slave定时从leader获取注册表更新
 */
public class ClusterSlaveRequestUpdateRegeditTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                String leader = ClusterService.CURRENTNODE.getClusterLeader().getAddress();
                Node node = ClusterService.CURRENTNODE.getLeaders().get(leader);
                //如果当前的leader同时也是自己的master，则需要定时同步注册表信息
                if (node != null)
                    SlaveService.requestUpdateClusterRegedit();
            } catch (Exception e) {
                log.error("ClusterSlaveRequestUpdateRegeditTask()->exception!", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getAdvanceConfig().getSlaveUpdateRegeditTime());
            } catch (InterruptedException e) {
                log.error("ClusterSlaveRequestUpdateRegeditTask()->exception!", e);
            }
        }
    }
}
