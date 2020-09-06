package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.follow.VoteSliceLeader;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSliceFollows;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
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
               Map<String, RegisterNode> brokers= ClusterLeaderService.BROKER_REGISTER_CENTER;
               Iterator<Map.Entry<String, RegisterNode>> items=brokers.entrySet().iterator();
               while (items.hasNext()){
                   Map.Entry<String, RegisterNode> item=items.next();
                   RegisterNode node=item.getValue();
                   ClusterService.getConfig().getClusterPool().submit(new Runnable() {
                       @Override
                       public void run() {
                           //分片leader节点失效。选新leader
                           if (DateUtils.isGreaterThanLoseTime(node.getLastHeartbeat())&&
                                   node.getNode().getFollows()!=null&&node.getNode().getFollows().size()>0) {
                               Node newleader=VoteSliceLeader.voteNewLeader(node.getNode().getFollows());
                               VoteSliceLeader.notifySliceFollowsNewLeader(node.getNode().getFollows(),newleader.getAddress(),node.getNode().getAddress(),3,5);
                               VoteSliceLeader.updateRegedit(brokers,node.getNode().getAddress());
                           }else if(node.getNode().getFollows()!=null&&node.getNode().getFollows().size()==0){

                           }
                       }
                   });
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
