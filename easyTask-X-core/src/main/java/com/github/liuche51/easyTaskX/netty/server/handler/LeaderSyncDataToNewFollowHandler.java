package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.follow.FollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;

public class LeaderSyncDataToNewFollowHandler extends BaseHandler {
    @Override
    public String process(Dto.Frame frame) throws Exception {
        ScheduleDto.ScheduleList scheduleList = ScheduleDto.ScheduleList.parseFrom(frame.getBodyBytes());
        FollowService.saveScheduleBakBatchByTran(scheduleList);
        return null;
    }
}
