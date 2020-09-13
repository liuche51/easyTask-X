package com.github.liuche51.easyTaskX.netty.server.handler.follow;

import com.github.liuche51.easyTaskX.cluster.follow.SliceFollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * 分片follow处理分片leader发来的尝试保持任务逻辑
 */
public class TranTrySaveTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ScheduleDto.Schedule schedule = ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
        SliceFollowService.trySaveTask(schedule);
        return null;
    }
}
