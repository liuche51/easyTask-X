package com.github.liuche51.easyTaskX.cluster.task.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteMaster;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSlave;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.BinlogClusterMetaDao;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.enume.RegNodeTypeEnum;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
                log.error("", e);
            }
            try {
                TimeUnit.SECONDS.sleep(NodeService.getConfig().getAdvanceConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("", e);
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
                            List<BinlogClusterMeta> binlogClusterMetas = new ArrayList<>(regNode.getSlaves().size() + 1);
                            binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.DELETE, RegNodeTypeEnum.REGBROKER, regNode.getAddress(), StringConstant.EMPTY));
                            //如果有Slaves。则选出新master，并通知它们。没有则直接移出注册表
                            RegNode newMaster = null;
                            if (regNode.getSlaves().size() > 0) {
                                newMaster = VoteMaster.voteNewMaster(regNode.getSlaves());
                                VoteMaster.updateRegedit(regNode);
                                Iterator<String> items = regNode.getSlaves().keySet().iterator();
                                while (items.hasNext()) {
                                    String item = items.next();
                                    binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.UPDATE, RegNodeTypeEnum.REGBROKER, item, JSONObject.toJSONString(brokers.get(item))));
                                }
                                LeaderService.notifySlaveVotedNewMaster(regNode.getSlaves(), newMaster.getAddress(), regNode.getAddress());
                            }
                            BinlogClusterMetaDao.saveBatch(binlogClusterMetas);
                            LeaderService.notifyFollowsUpdateRegedit(regNode.getSlaves(), StringConstant.BROKER);
                            LeaderService.notifyClinetsChangedBroker(regNode.getAddress(), newMaster == null ? null : newMaster.getAddress(), OperationTypeEnum.DELETE);
                        }
                        //master没失效，但是Slave失效了
                        else {
                            ConcurrentHashMap<String, RegNode> slaves = regNode.getSlaves();
                            //初始化，还没有一个Slave时，选出一批slave
                            if (slaves.size() == 0) {
                                try {
                                    List<RegNode> newSlaves = VoteSlave.initVoteSlaves(regNode);
                                    LeaderService.notifyFollowsUpdateRegedit(newSlaves, StringConstant.BROKER);
                                    List<BinlogClusterMeta> binlogClusterMetas = new ArrayList<>(newSlaves.size() + 1);
                                    binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.UPDATE, RegNodeTypeEnum.REGBROKER, regNode.getAddress(), JSONObject.toJSONString(regNode)));
                                    for (RegNode newslave : newSlaves) {
                                        binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.UPDATE, RegNodeTypeEnum.REGBROKER, newslave.getAddress(), JSONObject.toJSONString(LeaderService.BROKER_REGISTER_CENTER.get(newslave.getAddress()))));
                                    }
                                    BinlogClusterMetaDao.saveBatch(binlogClusterMetas);
                                    //如果当前节点是Leader自己选slave，则需要通知所有其他所有Follows更新备用Leader信息
                                    if (regNode.getAddress().equals(NodeService.CURRENTNODE.getClusterLeader().getAddress())) {
                                        LeaderService.notifyFollowsBakLeaderChanged();
                                    }
                                } catch (VotingException e) {
                                    log.info("normally exception!{}", e.getMessage());
                                } catch (Exception e) {
                                    log.error("", e);
                                }
                            }
                            //已经有Slaves时
                            else {
                                Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
                                while (items.hasNext()) {
                                    Map.Entry<String, RegNode> item = items.next();
                                    RegNode slave = item.getValue();
                                    RegBroker regSlave = brokers.get(slave.getAddress());
                                    //slave没有注册信息或者心跳超时了。（没有注册信息，可能是因为上面判断过程中已经将其移除注册表了）.心跳超时，这里不需要移除注册表操作，因为leader检查该slave注册表时会操作移除
                                    if (regSlave == null || DateUtils.isGreaterThanLoseTime(regSlave.getLastHeartbeat())) {
                                        try {
                                            RegNode newSlave = VoteSlave.voteNewSlave(regNode, slave);
                                            List<RegNode> nodes = new ArrayList<>(2);
                                            nodes.add(regNode);
                                            nodes.add(newSlave);
                                            LeaderService.notifyFollowsUpdateRegedit(nodes, StringConstant.BROKER);
                                            List<BinlogClusterMeta> binlogClusterMetas = new ArrayList<>(2);
                                            binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.UPDATE, RegNodeTypeEnum.REGBROKER, regNode.getAddress(), JSONObject.toJSONString(regNode)));
                                            binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.UPDATE, RegNodeTypeEnum.REGBROKER, newSlave.getAddress(), JSONObject.toJSONString(LeaderService.BROKER_REGISTER_CENTER.get(newSlave.getAddress()))));
                                            BinlogClusterMetaDao.saveBatch(binlogClusterMetas);
                                            //如果当前节点是Leader自己变更slave，则需要通知所有其他所有Follows更新备用Leader信息
                                            if (regNode.getAddress().equals(NodeService.CURRENTNODE.getClusterLeader().getAddress())) {
                                                LeaderService.notifyFollowsBakLeaderChanged();
                                            }
                                        } catch (VotingException e) {
                                            log.info("normally exception!{}", e.getMessage());
                                        } catch (Exception e) {
                                            log.error("", e);
                                        }
                                        //items.remove();这里不需要了。因为在voteNewSlave中已经移除了
                                    }
                                }
                            }

                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        }
    }

    /**
     * 处理Client注册表
     */
    private void dealClientRegedit() {
        Map<String, RegClient> clients = LeaderService.CLIENT_REGISTER_CENTER;
        Iterator<Map.Entry<String, RegClient>> items = clients.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegClient> item = items.next();
            RegClient regNode = item.getValue();
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        //clinet节点失效
                        if (DateUtils.isGreaterThanLoseTime(regNode.getLastHeartbeat())) {
                            LeaderService.CLIENT_REGISTER_CENTER.remove(regNode.getAddress());
                            LeaderService.notifyBrokersChangedClinet(regNode.getAddress(), OperationTypeEnum.DELETE);
                            List<BinlogClusterMeta> binlogClusterMetas = new ArrayList<>(1);
                            binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.DELETE, RegNodeTypeEnum.REGCLIENT, regNode.getAddress(), StringConstant.EMPTY));
                            BinlogClusterMetaDao.saveBatch(binlogClusterMetas);
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        }
    }
}
