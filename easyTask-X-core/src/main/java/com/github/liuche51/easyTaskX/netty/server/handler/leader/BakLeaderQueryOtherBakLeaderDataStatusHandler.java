package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.BakLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * bakleader响应：bakleader查询其他bakleader当前的数据状态
 */
public class BakLeaderQueryOtherBakLeaderDataStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) {
        return ByteString.copyFromUtf8(String.valueOf(BakLeaderService.DATA_STATUS));
    }
}