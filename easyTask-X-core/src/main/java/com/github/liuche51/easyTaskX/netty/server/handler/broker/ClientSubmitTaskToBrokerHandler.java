package com.github.liuche51.easyTaskX.netty.server.handler.broker;


import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.master.AnnularQueueTask;
import com.github.liuche51.easyTaskX.dto.InnerTask;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.TraceLogUtil;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * Broker响应：Client提交任务处理类
 * 1、将任务先放入队列，然后立即返回。防止长时间阻塞客户端线程
 */
public class ClientSubmitTaskToBrokerHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ScheduleDto.ScheduleList list=ScheduleDto.ScheduleList.parseFrom(frame.getBodyBytes()) ;
        List<ScheduleDto.Schedule> schedulesList = list.getSchedulesList();
        for(ScheduleDto.Schedule dto:schedulesList){
            TraceLogUtil.trace(dto.getId(),"服务收到客户端提交的任务");
            Schedule schedule=Schedule.valueOf(dto);
            MasterService.WAIT_SUBMIT_TASK.put(schedule);//这里使用阻塞接口插入队列。不能因为队列暂时满了，而丢弃元素或返回异常，效率低
            InnerTask innerTask=InnerTask.parseFromScheduleDto(dto);
            AnnularQueueTask.getInstance().submitAddSlice(innerTask);
        }

        return null;
    }
}
