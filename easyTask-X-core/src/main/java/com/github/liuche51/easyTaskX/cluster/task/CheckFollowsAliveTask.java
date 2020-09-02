package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteFollows;
import com.github.liuche51.easyTaskX.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.exception.VotedException;
import com.github.liuche51.easyTaskX.util.exception.VotingException;
import com.github.liuche51.easyTaskX.zk.ZKService;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点对zk的心跳。检查follows是否失效。
 * 失效则进入选举。选举后将原follow备份数据同步给新follow
 */
public class CheckFollowsAliveTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                ConcurrentHashMap<String, Node> follows = ClusterService.CURRENTNODE.getFollows();
                Iterator<Map.Entry<String, Node>> items = follows.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    Node oldFollow=item.getValue();
                    String path = StringConstant.CHAR_SPRIT+StringConstant.CHAR_SPRIT + oldFollow.getAddress();


                }
            }  catch (Exception e) {
                log.error("CheckFollowsAliveTask()", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("CheckFollowsAliveTask()", e);
            }
        }
    }
}
