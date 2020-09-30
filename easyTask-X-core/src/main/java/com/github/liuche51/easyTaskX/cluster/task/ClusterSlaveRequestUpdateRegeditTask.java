package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;

/**
 * 集群Slave定时从leader获取注册表更新
 */
public class ClusterSlaveRequestUpdateRegeditTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                SlaveService.requestUpdateClusterRegedit();
            } catch (Exception e) {
                log.error("ClusterSlaveRequestUpdateRegeditTask()->exception!", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getAdvanceConfig().getUpdateRegeditTime());
            } catch (InterruptedException e) {
                log.error("ClusterSlaveRequestUpdateRegeditTask()->exception!", e);
            }
        }
    }
}
