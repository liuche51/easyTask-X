package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.Map;

/**
 * leader响应:Follows通过定时任务更新自身注册表信息
 */
public class FollowRequestLeaderSendRegeditHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        switch (body){
            case StringConstant.BROKER:
                RegBroker node = LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
                NodeDto.Node.Builder nodeBuilder= LeaderUtil.packageBrokerRegeditInfo(node);
                return nodeBuilder.build().toByteString();
            case StringConstant.CLINET:
                RegBroker node1= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());

                break;

            default:break;
        }
        return null;
    }
}
