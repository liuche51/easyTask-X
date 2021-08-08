package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.cluster.task.leader.CheckFollowsAliveTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderService {
    private static final Logger log = LoggerFactory.getLogger(VoteSlave.class);
    /**
     * 集群BROKER注册表LeaderNotifyClientUpdateBrokerChange
     */
    public static ConcurrentHashMap<String, RegBroker> BROKER_REGISTER_CENTER = new ConcurrentHashMap<>(10);
    /**
     * 集群CLIENT注册表
     */
    public static ConcurrentHashMap<String, RegClient> CLIENT_REGISTER_CENTER = new ConcurrentHashMap<>(10);

    /**
     * 通知follows节点更新注册表信息
     *
     * @param nodes
     * @param type  节点类型  broker和client
     */
    public static void notifyFollowsUpdateRegedit(List<RegNode> nodes, String type) {

        nodes.forEach(x -> {
            LeaderUtil.notifyFollowUpdateRegedit(x.getAddress(), type);
        });
    }

    /**
     * 通知follows更新注册表信息
     *
     * @param nodes
     * @param type  节点类型  broker和client
     */
    public static void notifyFollowsUpdateRegedit(Map<String, RegNode> nodes, String type) {
        Iterator<Map.Entry<String, RegNode>> items = nodes.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyFollowUpdateRegedit(items.next().getValue().getAddress(), type);
        }
    }

    /**
     * leader通知其BakLeader 更新Broker类型注册表信息
     *
     * @param bakleaders 备用leader节点
     * @param brokers    brokers
     * @param opType     操作类型。新增、删除、更新
     */
    public static void notifyBakLeaderUpdateRegedit(Map<String, BaseNode> bakleaders, List<RegBroker> brokers, String opType) {
        brokers.forEach(x -> {
            LeaderUtil.notifyBakLeaderUpdateRegedit(bakleaders, x, opType);
        });
    }


    /**
     * leader通知其BakLeader 更新Client类型注册表信息
     *
     * @param bakleaders 备用leader节点
     * @param client     client
     * @param opType     操作类型。新增、删除、更新
     */
    public static void notifyBakLeaderUpdateRegedit(Map<String, BaseNode> bakleaders, RegClient client, String opType) {
        Iterator<Map.Entry<String, BaseNode>> items = bakleaders.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, BaseNode> item = items.next();
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBakLeaderUpdateRegedit)
                                .setSource(NodeService.CURRENTNODE.getAddress());
                        NodeDto.Node.Builder nodeBuilder = NodeDto.Node.newBuilder();
                        nodeBuilder.setPort(client.getPort());
                        nodeBuilder.setHost(client.getHost());
                        nodeBuilder.setExt(StringConstant.CLINET + StringConstant.CHAR_SPRIT_STRING + opType);
                        builder.setBodyBytes(nodeBuilder.build().toByteString());
                        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, item.getValue().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                        if (!ret)
                            log.info("normally exception!notifyBakLeaderUpdateRegedit() failed.");
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        }
        ;
    }

    /**
     * 通知Follow更新备用leader信息
     */
    public static void notifyFollowsBakLeaderChanged() {
        String bakLeader = JSONObject.toJSONString(NodeService.CURRENTNODE.getSlaves());
        Iterator<String> items = LeaderService.BROKER_REGISTER_CENTER.keySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyFollowBakLeaderChanged(items.next(), bakLeader);
        }
        Iterator<String> items2 = LeaderService.CLIENT_REGISTER_CENTER.keySet().iterator();
        while (items2.hasNext()) {
            LeaderUtil.notifyFollowBakLeaderChanged(items2.next(), bakLeader);
        }
    }

    /**
     * 通知Broker注册成功。
     *
     * @param broker
     */
    public static void notifyBrokerRegisterSucceeded(BaseNode broker) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBrokerRegisterSucceeded)
                            .setSource(NodeService.CURRENTNODE.getAddress());
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, broker.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyBrokerRegisterSucceeded() failed.");
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * 通知Clinets。Broker发生变更。
     *
     * @param broker
     * @param newMaster
     * @param type      add、delete
     */
    public static void notifyClinetsChangedBroker(String broker, String newMaster, String type) {
        Iterator<Map.Entry<String, RegClient>> items = LeaderService.CLIENT_REGISTER_CENTER.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyClinetChangedBroker(items.next().getValue(), broker, newMaster, type);
        }
    }

    /**
     * 通知Brokers更新Clinet列表变更信息
     *
     * @param client
     * @param type   add、delete
     */
    public static void notifyBrokersChangedClinet(String client, String type) {
        Iterator<Map.Entry<String, RegBroker>> items = LeaderService.BROKER_REGISTER_CENTER.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyBrokerChangedClient(items.next().getValue(), client, type);
        }
    }

    /**
     * leader通知slaves。旧Master失效，leader已选新Master。
     *
     * @param slaves
     * @param newMaster
     * @param oldMaster
     * @return
     */
    public static boolean notifySlaveVotedNewMaster(Map<String, RegNode> slaves, String newMaster, String oldMaster) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotiySlaveVotedNewMaster).setSource(NodeService.getConfig().getAddress())
                    .setBody(oldMaster + StringConstant.CHAR_SPRIT_STRING + newMaster);
            Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
            while (items.hasNext()) {
                Map.Entry<String, RegNode> item = items.next();
                RegNode node = item.getValue();
                boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                if (!ret)
                    log.info("normally exception!notifySlavesNewMaster() failed.");
            }
            return true;
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * leader通知master，已经选出新Slave。
     *
     * @param master
     * @param newSlaveAddress
     * @param oldSlaveAddress
     */
    public static void notifyMasterVoteNewSlave(RegBroker master, String newSlaveAddress, String oldSlaveAddress) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyMasterVoteNewSlave)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(newSlaveAddress + StringConstant.CHAR_SPRIT_STRING + oldSlaveAddress);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, master.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyMasterVoteNewSlave() failed.");
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * 启动leader检查所有follows是否存活任务
     */
    public static TimerTask startCheckFollowAliveTask() {
        CheckFollowsAliveTask task = new CheckFollowsAliveTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }
}
