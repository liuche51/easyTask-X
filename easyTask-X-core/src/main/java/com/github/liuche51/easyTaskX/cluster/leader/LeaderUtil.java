package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.Node;
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
import java.util.Map;

public class LeaderUtil {
    private static final Logger log = LoggerFactory.getLogger(VoteSlave.class);

    /**
     * 通知节点更新注册表信息
     */
    public static void notifyFollowUpdateRegedit(String address, String type) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    switch (type) {
                        case "broker":
                            RegBroker node = LeaderService.BROKER_REGISTER_CENTER.get(address);
                            NodeDto.Node.Builder nodeBuilder = packageBrokerRegeditInfo(node);
                            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit)
                                    .setSource(NodeService.CURRENTNODE.getAddress()).setBodyBytes(nodeBuilder.build().toByteString());
                            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                            if (!ret)
                                log.info("normally exception!notifyNodeUpdateRegedit() failed.");
                        case "client":
                            RegBroker node1 = LeaderService.BROKER_REGISTER_CENTER.get(address);

                            break;

                        default:
                            break;
                    }

                } catch (Exception e) {
                    log.error("notifyNodeUpdateRegedit()->exception!", e);
                }
            }
        });
    }

    /**
     * 包装Broker注册信息
     *
     * @param node
     */
    public static NodeDto.Node.Builder packageBrokerRegeditInfo(RegBroker node) {
        NodeDto.Node.Builder nodeBuilder = NodeDto.Node.newBuilder();
        //备用leader信息
        nodeBuilder.setBakleader(JSONObject.toJSONString(NodeService.CURRENTNODE.getSlaves()));
        //slaves
        NodeDto.NodeList.Builder slavesBuilder = NodeDto.NodeList.newBuilder();
        Iterator<Map.Entry<String, RegNode>> items2 = node.getSlaves().entrySet().iterator();
        while (items2.hasNext()) {
            Map.Entry<String, RegNode> item2 = items2.next();
            RegNode itNode = item2.getValue();
            NodeDto.Node.Builder followBuilder = NodeDto.Node.newBuilder();
            followBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
            slavesBuilder.addNodes(followBuilder.build());
        }
        nodeBuilder.setSalves(slavesBuilder.build());
        //masters
        NodeDto.NodeList.Builder mastersBuilder = NodeDto.NodeList.newBuilder();
        Iterator<Map.Entry<String, RegNode>> items3 = node.getMasters().entrySet().iterator();
        while (items3.hasNext()) {
            Map.Entry<String, RegNode> item3 = items3.next();
            RegNode itNode = item3.getValue();
            NodeDto.Node.Builder followBuilder3 = NodeDto.Node.newBuilder();
            followBuilder3.setHost(itNode.getHost()).setPort(itNode.getPort());
            mastersBuilder.addNodes(followBuilder3.build());
        }
        nodeBuilder.setMasters(mastersBuilder.build());
        return nodeBuilder;
    }

    /**
     * 通知Follow更新备用leader信息
     * @param address
     * @param bakLeader
     */
    public static void notifyFollowBakLeaderChanged(String address,String bakLeader) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyFollwUpdateBakLeaderInfo)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(bakLeader);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, new Node(address).getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyFollowBakLeaderChanged() failed.");
                } catch (Exception e) {
                    log.error("notifyFollowBakLeaderChanged()->exception!", e);
                }
            }
        });
    }
    /**
     * 通知Client。有Broker注册变更消息
     * @param client
     * @param broker
     * @param type add、delete
     */
    public static void notifyClinetChangedBroker(RegClient client,String broker, String type) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyClientUpdateBrokerChange)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(type+"|"+broker);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyClinetChangedBroker() failed.");
                } catch (Exception e) {
                    log.error("notifyClinetChangedBroker()->exception!", e);
                }
            }
        });
    }
    /**
     * 通知Broker。有Client注册变更消息
     * @param client
     * @param broker
     * @param type add、delete
     */
    public static void notifyBrokerChangedClient(RegBroker broker,String client, String type) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBrokersUpdateClientChange)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(type+"|"+client);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, broker.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyBrokerChangedClient() failed.");
                } catch (Exception e) {
                    log.error("notifyBrokerChangedClient()->exception!", e);
                }
            }
        });
    }
}
