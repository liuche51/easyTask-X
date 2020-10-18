package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * leader响应：master通知leader，已经完成对新follow的数据同步。请求更新数据同步状态
 */
public class MasterNotifyLeaderUpdateRegeditForDataStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split(StringConstant.CHAR_SPRIT_STRING);//地址|状态
        RegBroker regNode= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
        RegNode follow=regNode.getSlaves().get(items[0]);
        follow.setDataStatus(new Short(items[1]));
        return null;
    }
}
