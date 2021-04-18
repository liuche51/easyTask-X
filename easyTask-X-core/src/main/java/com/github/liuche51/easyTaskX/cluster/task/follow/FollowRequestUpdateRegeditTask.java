package com.github.liuche51.easyTaskX.cluster.task.follow;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;

import java.util.concurrent.TimeUnit;

/**
 * Follow节点定时从leader获取注册表更新
 */
public class FollowRequestUpdateRegeditTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                BrokerService.requestUpdateRegedit();
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                TimeUnit.MINUTES.sleep(NodeService.getConfig().getAdvanceConfig().getFollowUpdateRegeditTime());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
