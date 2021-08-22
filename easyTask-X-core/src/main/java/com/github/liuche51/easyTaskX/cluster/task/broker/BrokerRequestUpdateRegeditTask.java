package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;

import java.util.concurrent.TimeUnit;

/**
 * Broker节点定时从leader获取注册表更新
 */
public class BrokerRequestUpdateRegeditTask extends TimerTask {
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
