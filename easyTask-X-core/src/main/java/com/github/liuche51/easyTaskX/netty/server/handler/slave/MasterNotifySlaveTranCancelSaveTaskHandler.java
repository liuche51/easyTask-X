package com.github.liuche51.easyTaskX.netty.server.handler.slave;

import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * slave响应：处理master发来的取消任务保持逻辑
 */
public class MasterNotifySlaveTranCancelSaveTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String transactionId = frame.getBody();
        SlaveService.cancelSaveTask(transactionId);
        return null;
    }
}
