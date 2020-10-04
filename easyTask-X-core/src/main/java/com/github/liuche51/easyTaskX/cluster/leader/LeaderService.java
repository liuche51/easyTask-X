package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.task.CheckFollowsAliveTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
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
     * 集群BROKER注册表
     */
    public static ConcurrentHashMap<String, RegBroker> BROKER_REGISTER_CENTER = new ConcurrentHashMap<>(10);
    /**
     * 集群CLIENT注册表
     */
    public static ConcurrentHashMap<String, RegClient> CLIENT_REGISTER_CENTER = new ConcurrentHashMap<>(10);

    /**
     * 通知节点更新注册表信息
     *
     * @param nodes
     */
    public static void notifyFollowsUpdateRegedit(List<RegNode> nodes,String type) {

        nodes.forEach(x -> {
            LeaderUtil.notifyFollowUpdateRegedit(x.getAddress(),type);
        });
    }
    /**
     * 通知follows更新注册表信息
     *
     * @param nodes
     */
    public static void notifyFollowsUpdateRegedit(Map<String,RegNode> nodes,String type) {
        Iterator<Map.Entry<String, RegNode>> items = nodes.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyFollowUpdateRegedit(items.next().getValue().getAddress(),type);
        }
    }

    /**
     * leader通知其slave 更新Broker类型注册表信息
     *
     * @param nodes
     */
    public static void notifySalveUpdateRegedit(Map<String, Node> nodes, RegBroker node) {
        Iterator<Map.Entry<String, Node>> items = nodes.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Node> item = items.next();
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifySalveUpdateRegedit)
                                .setSource(NodeService.CURRENTNODE.getAddress());
                        NodeDto.Node.Builder nodeBuilder=NodeDto.Node.newBuilder();
                        //clients
                        NodeDto.NodeList.Builder clientsBuilder= NodeDto.NodeList.newBuilder();
                        Iterator<Map.Entry<String,RegNode>> items=node.getClients().entrySet().iterator();
                        while (items.hasNext()){
                            Map.Entry<String,RegNode> item=items.next();
                            RegNode itNode=item.getValue();
                            NodeDto.Node.Builder clientBuilder= NodeDto.Node.newBuilder();
                            clientBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                            clientsBuilder.addNodes(clientBuilder.build());
                        }
                        nodeBuilder.setClients(clientsBuilder.build());
                        //follows
                        NodeDto.NodeList.Builder followsBuilder= NodeDto.NodeList.newBuilder();
                        Iterator<Map.Entry<String,RegNode>> items2=node.getSlaves().entrySet().iterator();
                        while (items2.hasNext()){
                            Map.Entry<String,RegNode> item2=items2.next();
                            RegNode itNode=item2.getValue();
                            NodeDto.Node.Builder followBuilder= NodeDto.Node.newBuilder();
                            followBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                            if(itNode.getDataStatus()!=null)
                                followBuilder.setDataStatus(itNode.getDataStatus().toString());
                            followsBuilder.addNodes(followBuilder.build());
                        }
                        nodeBuilder.setSalves(followsBuilder.build());
                        //leaders
                        NodeDto.NodeList.Builder leadersBuilder= NodeDto.NodeList.newBuilder();
                        Iterator<Map.Entry<String,RegNode>> items3=node.getMasters().entrySet().iterator();
                        while (items3.hasNext()){
                            Map.Entry<String,RegNode> item3=items3.next();
                            RegNode itNode=item3.getValue();
                            NodeDto.Node.Builder followBuilder3= NodeDto.Node.newBuilder();
                            followBuilder3.setHost(itNode.getHost()).setPort(itNode.getPort());
                            leadersBuilder.addNodes(followBuilder3.build());
                        }
                        nodeBuilder.setMasters(leadersBuilder.build());
                        builder.setBodyBytes(nodeBuilder.build().toByteString());
                        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, item.getValue().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                        if (!ret)
                            log.info("normally exception!notifySalveUpdateRegedit() failed.");
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        };
    }

    /**
     * leader通知其slave 更新Client类型注册表信息
     *
     * @param nodes
     */
    public static void notifySalveUpdateRegedit(Map<String, Node> nodes, RegClient node) {
        Iterator<Map.Entry<String, Node>> items = nodes.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Node> item = items.next();
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifySalveUpdateRegedit)
                                .setSource(NodeService.CURRENTNODE.getAddress());
                        NodeDto.Node.Builder nodeBuilder=NodeDto.Node.newBuilder();
                        //Brokers
                        NodeDto.NodeList.Builder clientsBuilder= NodeDto.NodeList.newBuilder();
                        Iterator<Map.Entry<String,RegNode>> items=node.getBrokers().entrySet().iterator();
                        while (items.hasNext()){
                            Map.Entry<String,RegNode> item=items.next();
                            RegNode itNode=item.getValue();
                            NodeDto.Node.Builder clientBuilder= NodeDto.Node.newBuilder();
                            clientBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                            clientsBuilder.addNodes(clientBuilder.build());
                        }
                        nodeBuilder.setClients(clientsBuilder.build());
                        builder.setBodyBytes(nodeBuilder.build().toByteString());
                        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, item.getValue().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                        if (!ret)
                            log.info("normally exception!notifySalveUpdateRegedit() failed.");
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        };
    }

    /**
     * 通知Follow更新备用leader信息
     */
    public static void notifyFollowsBakLeaderChanged(){
        String bakLeader= JSONObject.toJSONString(NodeService.CURRENTNODE.getSlaves());
        Iterator<String> items = LeaderService.BROKER_REGISTER_CENTER.keySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyFollowBakLeaderChanged(items.next(),bakLeader);
        }
        Iterator<String> items2 = LeaderService.CLIENT_REGISTER_CENTER.keySet().iterator();
        while (items2.hasNext()) {
            LeaderUtil.notifyFollowBakLeaderChanged(items2.next(),bakLeader);
        }
    }
    /**
     * 启动leader检查所有follows是否存活任务
     */
    public static TimerTask startCheckFollowAliveTask() {
        CheckFollowsAliveTask task = new CheckFollowsAliveTask();
        task.start();
        return task;
    }
}
