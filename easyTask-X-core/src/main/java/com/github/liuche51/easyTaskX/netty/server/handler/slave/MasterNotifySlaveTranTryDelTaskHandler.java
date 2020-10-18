package com.github.liuche51.easyTaskX.netty.server.handler.slave;

import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * slave响应:处理master发来的删除任务逻辑
 */
public class MasterNotifySlaveTranTryDelTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String tranAndSchedule = frame.getBody();
        String[] item=tranAndSchedule.split(",");
        SlaveService.tryDelTask(item[0],item[1]);
        return null;
    }
}
