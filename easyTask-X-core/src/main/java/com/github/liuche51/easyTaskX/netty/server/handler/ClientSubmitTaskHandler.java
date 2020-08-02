package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.StringUtils;

/**
 * 客户端提交任务处理类
 */
public class ClientSubmitTaskHandler extends BaseHandler{

    @Override
    public String process(Dto.Frame frame) throws Exception {
        ScheduleDto.Schedule dto = ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
        Schedule schedule=Schedule.valueOf(dto);
        ClusterService.submitTask(schedule);
        return StringConstant.EMPTY;
    }
}
