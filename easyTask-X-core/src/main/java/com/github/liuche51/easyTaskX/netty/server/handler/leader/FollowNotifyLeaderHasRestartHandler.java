package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.RegNodeTypeEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;

/**
 * follow通知leader，已经重新启动了
 * 1、如果节点还在注册表中，说明还没有被踢出集群。则回复活着
 */
public class FollowNotifyLeaderHasRestartHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String address = frame.getSource();
        switch (body) {
            case StringConstant.BROKER:
                RegBroker registerNode = LeaderService.BROKER_REGISTER_CENTER.get(address);
                if (registerNode != null) {
                    registerNode.setLastHeartbeat(ZonedDateTime.now());
                    LeaderService.addFollowsHeartbeats(address, RegNodeTypeEnum.REGBROKER);
                    return ByteString.copyFromUtf8(StringConstant.ALIVE);
                }
            case StringConstant.CLINET:
                RegClient clientNode = LeaderService.CLIENT_REGISTER_CENTER.get(address);
                if (clientNode != null) {
                    clientNode.setLastHeartbeat(ZonedDateTime.now());
                    LeaderService.addFollowsHeartbeats(address, RegNodeTypeEnum.REGCLIENT);
                    return ByteString.copyFromUtf8(StringConstant.ALIVE);
                }
            default:
                break;
        }
        return null;
    }
}
