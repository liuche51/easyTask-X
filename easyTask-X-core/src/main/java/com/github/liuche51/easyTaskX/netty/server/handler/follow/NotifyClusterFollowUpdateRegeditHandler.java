package com.github.liuche51.easyTaskX.netty.server.handler.follow;

import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群分片follow处理来自集群leader注册表的同步通知。
 * 区分是Broker端还是Client端
 */
public class NotifyClusterFollowUpdateRegeditHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        NodeDto.Node node = NodeDto.Node.parseFrom(frame.getBodyBytes());
        String[] exts = node.getExt().split("|");//格式：客户端类型|操作类型
        if ("Broker".equalsIgnoreCase(exts[0])) {
            RegBroker regnode = new RegBroker(node.getHost(), node.getPort());
            if ("Delete".equalsIgnoreCase(exts[1]))
                ClusterLeaderService.BROKER_REGISTER_CENTER.remove(regnode.getAddress());
            else if ("Update".equalsIgnoreCase(exts[1])) {
                NodeDto.NodeList clientNodes = node.getClients();
                ConcurrentHashMap<String, RegNode> clients = new ConcurrentHashMap<>();
                clientNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    clients.put(regNode.getAddress(), regNode);
                });
                NodeDto.NodeList followNodes = node.getFollows();
                ConcurrentHashMap<String, RegNode> follows = new ConcurrentHashMap<>();
                followNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    if (x.hasDataStatus())
                        regNode.setDataStatus(Short.valueOf(x.getDataStatus()));
                    follows.put(regNode.getAddress(), regNode);
                });
                NodeDto.NodeList leaderNodes = node.getLeaders();
                ConcurrentHashMap<String, RegNode> leaders = new ConcurrentHashMap<>();
                leaderNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    leaders.put(regNode.getAddress(),regNode);
                });
                regnode.setClients(clients);
                regnode.setFollows(follows);
                regnode.setLeaders(leaders);
                regnode.setCreateTime(ZonedDateTime.now());
                regnode.setLastHeartbeat(ZonedDateTime.now());
                ClusterLeaderService.BROKER_REGISTER_CENTER.put(regnode.getAddress(), regnode);
            }
        } else if ("Client".equalsIgnoreCase(exts[0])) {
            RegClient regnode = new RegClient(node.getHost(), node.getPort());
            if ("Delete".equalsIgnoreCase(exts[1]))
                ClusterLeaderService.CLIENT_REGISTER_CENTER.remove(regnode.getAddress());
            else if ("Update".equalsIgnoreCase(exts[1])) {
                NodeDto.NodeList clientNodes = node.getBrokers();
                ConcurrentHashMap<String, RegNode> brokers = new ConcurrentHashMap<>();
                clientNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    brokers.put(regNode.getAddress(), regNode);
                });
                regnode.setBrokers(brokers);
                regnode.setCreateTime(ZonedDateTime.now());
                regnode.setLastHeartbeat(ZonedDateTime.now());
                ClusterLeaderService.CLIENT_REGISTER_CENTER.put(regnode.getAddress(), regnode);
            }
        }
        return null;
    }
}
