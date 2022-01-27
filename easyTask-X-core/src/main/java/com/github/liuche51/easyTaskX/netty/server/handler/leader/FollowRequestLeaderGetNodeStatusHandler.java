package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * leader响应:follow请求leader，获取当前节点状态
 */
public class FollowRequestLeaderGetNodeStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String address = frame.getSource();
        switch (body) {
            case StringConstant.BROKER:
                RegBroker registerNode = LeaderService.BROKER_REGISTER_CENTER.get(address);
                if (registerNode != null) {
                    return ByteString.copyFromUtf8(String.valueOf(registerNode.getNodeStatus()));
                }
            case StringConstant.CLINET:
                RegClient clientNode = LeaderService.CLIENT_REGISTER_CENTER.get(address);
                if (clientNode != null) {
                    return ByteString.copyFromUtf8(String.valueOf(clientNode.getNodeStatus()));
                }
            default:
                break;
        }
        return null;
    }
}
