package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.follow.VoteLeader;

import com.github.liuche51.easyTaskX.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.zk.ZKService;

import java.util.Iterator;
import java.util.Map;
/**
 * 节点对zk的心跳。检查leader是否失效。
 * 失效则进入选举
 */
public class CheckLeadersAliveTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    String path = StringConstant.CHAR_SPRIT+StringConstant.CHAR_SPRIT + item.getValue().getAddress();

                }
            } catch (Exception e) {
                log.error("CheckLeadersAliveTask()", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("CheckLeadersAliveTask()", e);
            }
        }
    }
}
