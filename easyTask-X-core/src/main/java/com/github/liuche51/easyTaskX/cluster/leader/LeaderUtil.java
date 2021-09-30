package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.*;
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
                        case StringConstant.BROKER:
                            RegBroker node = LeaderService.BROKER_REGISTER_CENTER.get(address);
                            NodeDto.Node.Builder nodeBuilder = packageBrokerRegeditInfo(node);
                            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit)
                                    .setSource(NodeService.CURRENTNODE.getAddress()).setBodyBytes(nodeBuilder.build().toByteString());
                            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                            if (!ret) {
                                NettyMsgService.writeRpcErrorMsgToDb("Leader通知Follow节点更新注册表信息。失败！", "com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil.notifyFollowUpdateRegedit");
                            }
                        case StringConstant.CLINET:
                            //目前没有客户端需要更新注册表逻辑

                            break;

                        default:
                            break;
                    }

                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * leader通知其BakLeader 更新Broker类型注册表信息
     *
     * @param bakleaders 备用leader节点
     * @param broker     broker
     * @param opType     操作类型。新增、删除、更新
     */
    public static void notifyBakLeaderUpdateRegedit(Map<String, BaseNode> bakleaders, RegBroker broker, String opType) {
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
                        switch (opType) {
                            case StringConstant.ADD:
                            case StringConstant.DELETE:
                                break;
                            case StringConstant.UPDATE:
                                //slaves
                                NodeDto.NodeList.Builder slavesBuilder = NodeDto.NodeList.newBuilder();
                                Iterator<Map.Entry<String, RegNode>> items2 = broker.getSlaves().entrySet().iterator();
                                while (items2.hasNext()) {
                                    Map.Entry<String, RegNode> item2 = items2.next();
                                    RegNode itNode = item2.getValue();
                                    NodeDto.Node.Builder followBuilder = NodeDto.Node.newBuilder();
                                    followBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                                    if (itNode.getDataStatus() != null)
                                        followBuilder.setDataStatus(itNode.getDataStatus().toString());
                                    slavesBuilder.addNodes(followBuilder.build());
                                }
                                nodeBuilder.setSalves(slavesBuilder.build());
                                //masters
                                NodeDto.NodeList.Builder mastersBuilder = NodeDto.NodeList.newBuilder();
                                Iterator<Map.Entry<String, RegNode>> items3 = broker.getMasters().entrySet().iterator();
                                while (items3.hasNext()) {
                                    Map.Entry<String, RegNode> item3 = items3.next();
                                    RegNode itNode = item3.getValue();
                                    NodeDto.Node.Builder followBuilder3 = NodeDto.Node.newBuilder();
                                    followBuilder3.setHost(itNode.getHost()).setPort(itNode.getPort());
                                    mastersBuilder.addNodes(followBuilder3.build());
                                }
                                nodeBuilder.setMasters(mastersBuilder.build());
                                break;
                            default:
                                break;
                        }
                        nodeBuilder.setExt(StringConstant.CLINET + StringConstant.CHAR_SPRIT_STRING + opType);
                        builder.setBodyBytes(nodeBuilder.build().toByteString());
                        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, item.getValue().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                        if (!ret) {
                            NettyMsgService.writeRpcErrorMsgToDb("leader通知其BakLeader 更新Broker类型注册表信息。失败！", "com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil.notifyBakLeaderUpdateRegedit");
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        }
        ;
    }

    /**
     * 打包装Broker所需要更新的所有注册信息
     * 1、备用leader
     * 2、最新master
     * 3、最新slave
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
     *
     * @param address
     * @param bakLeader
     */
    public static void notifyFollowBakLeaderChanged(String address, String bakLeader) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyFollowUpdateBakLeaderInfo)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(bakLeader);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, new Node(address).getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyFollowBakLeaderChanged() failed.");
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * 通知Clinets。Broker发生变更。
     *
     * @param client
     * @param broker
     * @param type   add、delete
     */
    public static void notifyClinetChangedBroker(RegClient client, String broker, String newMaster, String type) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    StringBuilder str = new StringBuilder(broker);
                    if (!StringUtils.isNullOrEmpty(newMaster))
                        str.append(StringConstant.CHAR_SPRIT_STRING).append(newMaster);
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyClientBrokerChanged)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(type + str.toString());//type+Broker地址+新master地址
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret) {
                       NettyMsgService.writeRpcErrorMsgToDb("Leader通知Clinets。Broker发生变更。失败！","com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil.notifyClinetChangedBroker");
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * 通知Broker。有Client注册变更消息
     *
     * @param client
     * @param broker
     * @param type   add、delete
     */
    public static void notifyBrokerChangedClient(RegBroker broker, String client, String type) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBrokerClientChanged)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(type + StringConstant.CHAR_SPRIT_STRING + client);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, broker.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret) {
                        NettyMsgService.writeRpcErrorMsgToDb("Leader通知Broker。有Client注册变更消息。失败！","com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil.notifyBrokerChangedClient");
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }
}
