package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;

public class GetRegisteredBokersHandler extends BaseHandler {
    @Override
    public String process(Dto.Frame frame) throws Exception {
        return JSONObject.toJSONString(ClusterLeaderService.getRegisteredBokers());
    }
}
