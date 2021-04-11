package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Leader接受响应：broker通知leader，已经完成重新分配任务至新client以及salve的数据同步。请求更新数据同步状态
 */
public class BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        RegBroker regNode= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
        regNode.setReDispatchTaskStatus(Short.parseShort(body));
        return null;
    }
}