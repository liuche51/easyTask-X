package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.google.protobuf.ByteString;

/**
 * 客户端删除任务处理类
 * 系统自动删除已执行完的任务使用
 */
public class ClientDeleteTaskHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String taskId = frame.getBody();
        boolean ret= NodeService.deleteTask(taskId);
        if(!ret) throw new Exception("deleteTask()-> exception!");
        return ByteString.copyFromUtf8(taskId);
    }
}
