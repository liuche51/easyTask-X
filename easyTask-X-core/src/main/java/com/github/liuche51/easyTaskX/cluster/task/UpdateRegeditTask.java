package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;

/**
 * 节点定时从集群leader获取注册表更新
 */
public class UpdateRegeditTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                ClusterLeaderService.requestUpdateRegedit();
            } catch (Exception e) {
                log.error("UpdateRegeditTask()->exception!", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getAdvanceConfig().getUpdateRegeditTime());
            } catch (InterruptedException e) {
                log.error("UpdateRegeditTask()->exception!", e);
            }
        }
    }
}
