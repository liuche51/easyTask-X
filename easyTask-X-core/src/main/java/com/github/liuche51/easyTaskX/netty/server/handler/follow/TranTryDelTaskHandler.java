package com.github.liuche51.easyTaskX.netty.server.handler.follow;

import com.github.liuche51.easyTaskX.cluster.follow.SliceFollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * 分片follow处理分片leader发来的删除任务逻辑
 */
public class TranTryDelTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String tranAndSchedule = frame.getBody();
        String[] item=tranAndSchedule.split(",");
        SliceFollowService.tryDelTask(item[0],item[1]);
        return null;
    }
}
