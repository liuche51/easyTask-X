package com.github.liuche51.easyTaskX.netty.server.handler.slave;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群slave响应：处理来自leader注册表的同步通知。
 * 区分是Broker端还是Client端
 */
public class LeaderNotifySalveUpdateRegeditHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        NodeDto.Node node = NodeDto.Node.parseFrom(frame.getBodyBytes());
        String[] exts = node.getExt().split("|");//格式：客户端类型|操作类型
        if (StringConstant.BROKER.equalsIgnoreCase(exts[0])) {
            RegBroker regnode = new RegBroker(node.getHost(), node.getPort());
            if (StringConstant.DELETE.equalsIgnoreCase(exts[1]))
                LeaderService.BROKER_REGISTER_CENTER.remove(regnode.getAddress());
            else if (StringConstant.UPDATE.equalsIgnoreCase(exts[1])) {
                NodeDto.NodeList slaveNodes = node.getSalves();
                ConcurrentHashMap<String, RegNode> follows = new ConcurrentHashMap<>();
                slaveNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    if (x.hasDataStatus())
                        regNode.setDataStatus(Short.valueOf(x.getDataStatus()));
                    follows.put(regNode.getAddress(), regNode);
                });
                NodeDto.NodeList masterNodes = node.getMasters();
                ConcurrentHashMap<String, RegNode> leaders = new ConcurrentHashMap<>();
                masterNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    leaders.put(regNode.getAddress(),regNode);
                });
                regnode.setSlaves(follows);
                regnode.setMasters(leaders);
                regnode.setCreateTime(ZonedDateTime.now());
                regnode.setLastHeartbeat(ZonedDateTime.now());
                LeaderService.BROKER_REGISTER_CENTER.put(regnode.getAddress(), regnode);
            }
        } else if (StringConstant.CLINET.equalsIgnoreCase(exts[0])) {
            RegClient regnode = new RegClient(node.getHost(), node.getPort());
            if (StringConstant.DELETE.equalsIgnoreCase(exts[1]))
                LeaderService.CLIENT_REGISTER_CENTER.remove(regnode.getAddress());
            else if (StringConstant.UPDATE.equalsIgnoreCase(exts[1])) {
                NodeDto.NodeList clientNodes = node.getBrokers();
                ConcurrentHashMap<String, RegNode> brokers = new ConcurrentHashMap<>();
                clientNodes.getNodesList().forEach(x -> {
                    RegNode regNode = new RegNode(x.getHost(), x.getPort());
                    brokers.put(regNode.getAddress(), regNode);
                });
                regnode.setBrokers(brokers);
                regnode.setCreateTime(ZonedDateTime.now());
                regnode.setLastHeartbeat(ZonedDateTime.now());
                LeaderService.CLIENT_REGISTER_CENTER.put(regnode.getAddress(), regnode);
            }
        }
        return null;
    }
}
