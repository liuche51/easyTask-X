package com.github.liuche51.easyTaskX.netty.server.handler.follow;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Follow接收Leader的更新备用leader信息
 */
public class LeaderNotifyFollwUpdateBakLeaderHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String info=frame.getBody();
        NodeService.CURRENTNODE.setBakLeader(info);
        return null;
    }
}
