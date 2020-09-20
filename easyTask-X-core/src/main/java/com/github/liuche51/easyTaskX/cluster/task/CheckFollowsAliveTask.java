package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSliceLeader;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSliceFollows;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群leader检查follows存活状态
 */
public class CheckFollowsAliveTask extends TimerTask {
    //是否已经存在一个任务实例运行中
    public static volatile boolean hasRuning=false;
    @Override
    public void run() {
        while (!isExit()) {
            try {
                dealBrokerRegedit();
            } catch (Exception e) {
                log.error("CheckFollowsAliveTask()", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getAdvanceConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("CheckFollowsAliveTask()", e);
            }
        }
    }

    /**
     * 处理服务端Broker节点的存活逻辑
     */
    private void dealBrokerRegedit() {
        Map<String, RegBroker> brokers = ClusterLeaderService.BROKER_REGISTER_CENTER;
        Iterator<Map.Entry<String, RegBroker>> items = brokers.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegBroker> item = items.next();
            RegBroker regNode = item.getValue();
            ClusterService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    //分片leader节点失效,且有follows。选新leader
                    if (DateUtils.isGreaterThanLoseTime(regNode.getLastHeartbeat())) {
                        //如果有follows。则选出新分片leader，并通知它们。没有则直接移出注册表
                        if (regNode.getFollows().size() > 0) {
                            RegNode newleader = VoteSliceLeader.voteNewLeader(regNode.getFollows());
                            VoteSliceLeader.notifySliceFollowsNewLeader(regNode.getFollows(), newleader.getAddress(), regNode.getAddress());
                        }
                        VoteSliceLeader.updateRegedit(regNode);
                        ClusterLeaderService.notifyNodesUpdateRegedit(regNode.getFollows());
                        ClusterLeaderService.notifyClusterFollowUpdateRegedit(ClusterService.CURRENTNODE.getFollows(),regNode);

                    }
                    //分片leader没失效，但是follow失效了
                    else {
                        ConcurrentHashMap<String, RegNode> follows = regNode.getFollows();
                        //初始化，还没有一个follow时
                        if (follows.size() == 0) {
                            try {
                                List<RegNode> newFollows= VoteSliceFollows.initVoteFollows(regNode);
                                ClusterLeaderService.notifyNodesUpdateRegedit(newFollows);
                            } catch (VotingException e) {
                                log.info("normally exception!{}", e.getMessage());
                            } catch (Exception e) {
                                log.error("initVoteFollows()->exception!", e);
                            }
                        }
                        //已经有follows时
                        else {
                            Iterator<Map.Entry<String, RegNode>> items = follows.entrySet().iterator();
                            while (items.hasNext()) {
                                Map.Entry<String, RegNode> item = items.next();
                                RegNode node = item.getValue();
                                RegBroker regNodeFollow = brokers.get(node.getAddress());
                                //follow没有注册信息或者心跳超时了。（没有注册信息，可能是因为上面判断过程中已经将其移除注册表了）
                                if (regNodeFollow == null || DateUtils.isGreaterThanLoseTime(regNodeFollow.getLastHeartbeat())) {
                                    try {
                                        RegNode newNode = VoteSliceFollows.voteNewFollow(regNode, node);
                                        VoteSliceFollows.notifySliceLeaderVoteNewFollow(regNode, newNode.getAddress(), node.getAddress());
                                        ClusterLeaderService.notifyNodesUpdateRegedit(Collections.singletonList(newNode));
                                    } catch (VotingException e) {
                                        log.info("normally exception!{}", e.getMessage());
                                    } catch (Exception e) {
                                        log.error("voteNewFollow()->exception!", e);
                                    }
                                    //items.remove();这里不需要了。因为在voteNewFollow中已经移除了
                                }
                            }
                        }

                    }
                }
            });
        }
    }
}
