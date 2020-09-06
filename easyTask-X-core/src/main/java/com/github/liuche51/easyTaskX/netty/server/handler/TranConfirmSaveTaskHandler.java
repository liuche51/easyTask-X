package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.follow.SliceFollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

public class TranConfirmSaveTaskHandler  extends BaseHandler{
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String transactionId = frame.getBody();
        SliceFollowService.confirmSaveTask(transactionId);
        return null;
    }
}
