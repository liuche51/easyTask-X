package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.DataStatusEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

/**
 * leader响应:follow请求leader，获取当前节点状态
 */
public class FollowRequestLeaderGetRegNodeStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String address = frame.getSource();
        RegNode node = null;
        switch (body) {
            case StringConstant.BROKER:
                node = LeaderService.BROKER_REGISTER_CENTER.get(address);
                break;
            case StringConstant.CLINET:
                node = LeaderService.CLIENT_REGISTER_CENTER.get(address);
                break;
            default:
                break;
        }
        if (node != null) {
            Map<String, Integer> map = new HashMap<>(Util.getMapInitCapacity(2));
            map.put(StringConstant.DATASTATUS, node.getDataStatus());
            map.put(StringConstant.NODESTATUS, node.getNodeStatus());
            return ByteString.copyFromUtf8(JSONObject.toJSONString(map));
        }

        return null;
    }
}
