package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.Map;

/**
 * leader响应ollows更新注册表信息
 * Follow的定时任务使用
 */
public class FollowRequestUpdateRegeditHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        switch (body){
            case "broker":
                RegBroker node = LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
                NodeDto.Node.Builder nodeBuilder= LeaderUtil.packageBrokerRegeditInfo(node);
                return nodeBuilder.build().toByteString();
            case "client":
                RegBroker node1= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());

                break;

            default:break;
        }
        return null;
    }
}
