package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.follow.VoteSliceLeader;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSliceFollows;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;

import java.util.Collections;
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
    private void dealBrokerRegedit(){
        Map<String, RegisterNode> brokers= ClusterLeaderService.BROKER_REGISTER_CENTER;
        Iterator<Map.Entry<String, RegisterNode>> items=brokers.entrySet().iterator();
        while (items.hasNext()){
            Map.Entry<String, RegisterNode> item=items.next();
            RegisterNode regNode=item.getValue();
            ClusterService.getConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    //分片leader节点失效,且有follows。选新leader
                    if (DateUtils.isGreaterThanLoseTime(regNode.getLastHeartbeat())) {
                        if(regNode.getNode().getFollows().size()>0){//如果有follows
                            Node newleader=VoteSliceLeader.voteNewLeader(regNode.getNode().getFollows());
                            VoteSliceLeader.notifySliceFollowsNewLeader(regNode.getNode().getFollows(),newleader.getAddress(),regNode.getNode().getAddress(),3,5);
                            VoteSliceLeader.updateRegedit(brokers,regNode.getNode().getAddress());
                        }else {
                            items.remove();//如果没有follows则直接移除
                        }

                    }else {
                        ConcurrentHashMap<String, Node> follows=regNode.getNode().getFollows();
                        Iterator<Map.Entry<String, Node>> items=follows.entrySet().iterator();
                        while (items.hasNext()) {
                            Map.Entry<String, Node> item = items.next();
                            Node node = item.getValue();
                            RegisterNode regNodeFollow=brokers.get(node.getAddress());
                            //follow没有注册信息或者心跳超时了。（没有注册信息，可能是因为上面判断过程中已经将其移除注册表了）
                            if (regNodeFollow==null||DateUtils.isGreaterThanLoseTime(regNodeFollow.getLastHeartbeat())) {
                                try {
                                    Node newNode=VoteSliceFollows.voteNewFollow(regNode,node);
                                    ClusterLeaderService.notifyNodeUpdateRegedit(Collections.singletonList(newNode));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                items.remove();
                            }
                        }
                    }
                }
            });
        }
    }
}
