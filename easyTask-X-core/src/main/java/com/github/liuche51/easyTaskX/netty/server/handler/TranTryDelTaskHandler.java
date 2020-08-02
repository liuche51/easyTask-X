package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.follow.FollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;

public class TranTryDelTaskHandler extends BaseHandler {
    @Override
    public String process(Dto.Frame frame) throws Exception {
        String tranAndSchedule = frame.getBody();
        String[] item=tranAndSchedule.split(",");
        FollowService.tryDelTask(item[0],item[1]);
        return null;
    }
}
