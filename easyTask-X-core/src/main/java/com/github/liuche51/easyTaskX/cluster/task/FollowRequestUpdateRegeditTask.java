package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;

/**
 * 节点定时从leader获取注册表更新
 */
public class FollowRequestUpdateRegeditTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                BrokerService.requestUpdateRegedit();
            } catch (Exception e) {
                log.error("FollowRequestUpdateRegeditTask()->exception!", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getAdvanceConfig().getFollowUpdateRegeditTime());
            } catch (InterruptedException e) {
                log.error("FollowRequestUpdateRegeditTask()->exception!", e);
            }
        }
    }
}
