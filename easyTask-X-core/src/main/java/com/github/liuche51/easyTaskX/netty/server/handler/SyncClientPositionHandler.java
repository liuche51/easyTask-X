package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.client.ClientService;
import com.github.liuche51.easyTaskX.cluster.follow.FollowService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.util.StringConstant;

/**
 * 同步客户端节点位置信息
 */
public class SyncClientPositionHandler extends BaseHandler {
    @Override
    public String process(Dto.Frame frame) throws Exception {
        String ret = frame.getBody();
        ClientService.updateClientPosition(ret);
        return StringConstant.EMPTY;
    }
}
