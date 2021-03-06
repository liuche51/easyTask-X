package com.github.liuche51.easyTaskX.netty.server.handler.broker;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Broker响应：Client提交任务处理类
 */
public class ClientNotifyBrokerSubmitTaskHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ScheduleDto.Schedule dto = ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
        Schedule schedule=Schedule.valueOf(dto);
        NodeService.submitTask(schedule);
        return null;
    }
}
