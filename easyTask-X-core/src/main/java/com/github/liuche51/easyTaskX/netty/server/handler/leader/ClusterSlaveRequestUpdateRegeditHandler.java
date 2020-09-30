package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * leader响应集群slave获取注册表信息
 */
public class ClusterSlaveRequestUpdateRegeditHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String item0=JSONObject.toJSONString(LeaderService.BROKER_REGISTER_CENTER);
        String item1=JSONObject.toJSONString(LeaderService.CLIENT_REGISTER_CENTER);
        return ByteString.copyFromUtf8(item0+"|"+item1);
    }
}
