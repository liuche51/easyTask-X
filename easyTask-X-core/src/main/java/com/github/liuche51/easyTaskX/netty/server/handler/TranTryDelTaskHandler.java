package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.follow.SliceFollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

public class TranTryDelTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String tranAndSchedule = frame.getBody();
        String[] item=tranAndSchedule.split(",");
        SliceFollowService.tryDelTask(item[0],item[1]);
        return null;
    }
}
