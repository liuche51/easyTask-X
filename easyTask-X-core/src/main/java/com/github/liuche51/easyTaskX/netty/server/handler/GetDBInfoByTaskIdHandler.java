package com.github.liuche51.easyTaskX.netty.server.handler;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.monitor.DBMonitor;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

public class GetDBInfoByTaskIdHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String tranId = frame.getBody();
        Map<String, List> map = DBMonitor.getInfoByTaskId(tranId);
        return ByteString.copyFromUtf8(JSONObject.toJSONString(map));
    }
}
