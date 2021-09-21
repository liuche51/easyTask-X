package com.github.liuche51.easyTaskX.netty.server.handler.broker;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Broker响应：Client删除任务处理类
 * 系统自动删除已执行完的任务使用
 */
public class ClientNotifyBrokerDeleteTaskHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String taskId = frame.getBody();
        boolean ret= BrokerService.deleteTask(taskId);
        if(!ret) throw new Exception("ret=false");
        return ByteString.copyFromUtf8(taskId);
    }
}
