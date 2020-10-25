package com.github.liuche51.easyTaskX.netty.server.handler.slave;

import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * slave响应:处理master发来的更新任务逻辑
 */
public class MasterNotifySlaveTranTryUpdateTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String tranAndSchedule = frame.getBody();
        String[] item=tranAndSchedule.split(StringConstant.CHAR_SPRIT_STRING);//transactionId+taskIds+values
        SlaveService.tryUpdateTask(item[0],item[1],item[2]);
        return null;
    }
}
