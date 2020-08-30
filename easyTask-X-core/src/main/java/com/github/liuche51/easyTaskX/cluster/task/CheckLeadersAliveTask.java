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
                    ZKNode node = ZKService.getDataByPath(path);
                    if (node == null)//防止leader节点已经不在zk。此处不需要选leader
                    {
                        log.info("CheckLeadersAliveTask():leader is not exist in zk");
                        items.remove();
                        continue;
                    }
                    //如果最后心跳时间超过60s，则直接删除该节点信息。并从自己的leader集合中移除掉
                    if (DateUtils.isGreaterThanLoseTime(node.getLastHeartbeat(),item.getValue().getClockDiffer().getDifferSecond())) {
                        items.remove();
                        ZKService.deleteNodeByPathIgnoreResult(path);
                    }
                    //如果最后心跳时间超过30s，进入选举新leader流程。并从自己的leader集合中移除掉
                    else if (DateUtils.isGreaterThanDeadTime(node.getLastHeartbeat(),item.getValue().getClockDiffer().getDifferSecond())) {
                        log.info("heartBeatToLeader():start to selectNewLeader");
                        VoteLeader.selectNewLeader(node, item.getValue().getAddress());
                        items.remove();
                    }

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
