package com.github.liuche51.easyTaskX.netty.server.handler.slave;

import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * slave响应:处理master发来的尝试保持任务逻辑
 */
public class MasterNotifySlaveTranTrySaveTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ScheduleDto.Schedule schedule = ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
        SlaveService.trySaveTask(schedule);
        return null;
    }
}
