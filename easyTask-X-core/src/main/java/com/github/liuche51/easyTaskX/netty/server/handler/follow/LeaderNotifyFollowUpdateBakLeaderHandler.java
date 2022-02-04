package com.github.liuche51.easyTaskX.netty.server.handler.follow;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Follow响应：接收Leader主动通知的更新备用leader信息
 */
public class LeaderNotifyFollowUpdateBakLeaderHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String info=frame.getBody();
        BrokerService.BAK_LEADER=info;
        return null;
    }
}
