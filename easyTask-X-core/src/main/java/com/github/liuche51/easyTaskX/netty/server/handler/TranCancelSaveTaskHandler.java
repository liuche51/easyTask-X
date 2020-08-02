package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.follow.FollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.util.StringConstant;

public class TranCancelSaveTaskHandler extends BaseHandler{
    @Override
    public String process(Dto.Frame frame) throws Exception {
        String transactionId = frame.getBody();
        FollowService.cancelSaveTask(transactionId);
        return StringConstant.EMPTY;
    }
}
