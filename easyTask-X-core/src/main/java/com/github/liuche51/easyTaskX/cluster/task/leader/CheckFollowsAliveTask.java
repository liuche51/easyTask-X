package com.github.liuche51.easyTaskX.cluster.task.leader;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteMaster;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSlave;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * leader检查follows存活状态
 */
public class CheckFollowsAliveTask extends TimerTask {
    //是否已经存在一个任务实例运行中
    public static volatile boolean hasRuning = false;

    @Override
    public void run() {
        while (!isExit()) {
            try {
                dealBrokerRegedit();
                dealClientRegedit();
            } catch (Exception e) {
                log.error("CheckFollowsAliveTask()", e);
            }
            try {
                Thread.sleep(NodeService.getConfig().getAdvanceConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("CheckFollowsAliveTask()", e);
            }
        }
    }

    /**
     * 处理服务端Broker节点的存活逻辑
     */
    private void dealBrokerRegedit() {
        Map<String, RegBroker> brokers = LeaderService.BROKER_REGISTER_CENTER;
        Iterator<Map.Entry<String, RegBroker>> items = brokers.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegBroker> item = items.next();
            RegBroker regNode = item.getValue();
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        //master节点失效,且有Slaves。选新master
                        if (DateUtils.isGreaterThanLoseTime(regNode.getLastHeartbeat())) {
                            //如果有Slaves。则选出新master，并通知它们。没有则直接移出注册表
                            RegNode newleader=null;
                            if (regNode.getSlaves().size() > 0) {
                                newleader = VoteMaster.voteNewMaster(regNode.getSlaves());
                                LeaderService.notifySlavesNewMaster(regNode.getSlaves(), newleader.getAddress(), regNode.getAddress());
                            }
                            VoteMaster.updateRegedit(regNode);
                            LeaderService.notifyFollowsUpdateRegedit(regNode.getSlaves(), StringConstant.BROKER);
                            LeaderService.notifyBakLeaderUpdateRegedit(NodeService.CURRENTNODE.getSlaves(), regNode);
                            LeaderService.notifyClinetsChangedBroker(regNode.getAddress(),newleader==null?null:newleader.getAddress(), StringConstant.DELETE);

                        }
                        //master没失效，但是Slave失效了
                        else {
                            ConcurrentHashMap<String, RegNode> slaves = regNode.getSlaves();
                            //初始化，还没有一个Slave时，选出一批slave
                            if (slaves.size() == 0) {
                                try {
                                    List<RegNode> newSlaves = VoteSlave.initVoteSlaves(regNode);
                                    LeaderService.notifyFollowsUpdateRegedit(newSlaves, StringConstant.BROKER);
                                    //如果当前节点是Leader自己选slave，则需要通知所有其他所有Follows更新备用Leader信息
                                    if (regNode.getAddress().equals(NodeService.CURRENTNODE.getClusterLeader().getAddress())) {
                                        LeaderService.notifyFollowsBakLeaderChanged();
                                    }
                                } catch (VotingException e) {
                                    log.info("normally exception!{}", e.getMessage());
                                } catch (Exception e) {
                                    log.error("initVoteFollows()->exception!", e);
                                }
                            }
                            //已经有Slaves时
                            else {
                                Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
                                while (items.hasNext()) {
                                    Map.Entry<String, RegNode> item = items.next();
                                    RegNode node = item.getValue();
                                    RegBroker regNodeFollow = brokers.get(node.getAddress());
                                    //slave没有注册信息或者心跳超时了。（没有注册信息，可能是因为上面判断过程中已经将其移除注册表了）
                                    if (regNodeFollow == null || DateUtils.isGreaterThanLoseTime(regNodeFollow.getLastHeartbeat())) {
                                        try {
                                            RegNode newSlave = VoteSlave.voteNewSlave(regNode, node);
                                            LeaderService.notifyMasterVoteNewSlave(regNode, newSlave.getAddress(), node.getAddress());
                                            LeaderService.notifyFollowsUpdateRegedit(Collections.singletonList(newSlave), StringConstant.BROKER);
                                            //如果当前节点是Leader自己变更slave，则需要通知所有其他所有Follows更新备用Leader信息
                                            if (regNode.getAddress().equals(NodeService.CURRENTNODE.getClusterLeader().getAddress())) {
                                                LeaderService.notifyFollowsBakLeaderChanged();
                                            }
                                        } catch (VotingException e) {
                                            log.info("normally exception!{}", e.getMessage());
                                        } catch (Exception e) {
                                            log.error("voteNewSlave()->exception!", e);
                                        }
                                        //items.remove();这里不需要了。因为在voteNewSlave中已经移除了
                                    }
                                }
                            }

                        }
                    } catch (Exception e) {
                        log.error("dealBrokerRegedit()->exception!", e);
                    }
                }
            });
        }
    }

    /**
     * 处理Client注册表
     */
    private void dealClientRegedit() {
        Map<String, RegClient> brokers = LeaderService.CLIENT_REGISTER_CENTER;
        Iterator<Map.Entry<String, RegClient>> items = brokers.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegClient> item = items.next();
            RegClient regNode = item.getValue();
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        //master节点失效,且有Slaves。选新master
                        if (DateUtils.isGreaterThanLoseTime(regNode.getLastHeartbeat())) {
                            LeaderService.CLIENT_REGISTER_CENTER.remove(regNode.getAddress());
                            LeaderService.notifyBrokersChangedClinet(regNode.getAddress(), StringConstant.DELETE);
                        }
                    } catch (Exception e) {
                        log.error("dealClientRegedit()->exception!", e);
                    }
                }
            });
        }
    }
}
