package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.util.StringConstant;

/**
 * 客户端删除任务处理类
 * 系统自动删除已执行完的任务使用
 */
public class ClientDeleteTaskHandler extends BaseHandler {

    @Override
    public String process(Dto.Frame frame) throws Exception {
        String taskId = frame.getBody();
        boolean ret=ClusterService.deleteTask(taskId);
        if(!ret) throw new Exception("deleteTask()-> exception!");
        return taskId;
    }
}
