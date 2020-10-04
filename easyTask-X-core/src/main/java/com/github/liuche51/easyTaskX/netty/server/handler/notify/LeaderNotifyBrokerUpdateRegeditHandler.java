package com.github.liuche51.easyTaskX.netty.server.handler.notify;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Broker处理leader通知当前节点更新注册表信息。
 */
public class LeaderNotifyBrokerUpdateRegeditHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        NodeDto.Node node = NodeDto.Node.parseFrom(frame.getBodyBytes());
        BrokerService.dealUpdate(node);
        return null;
    }
}
