package com.github.liuche51.easyTaskX.netty.server.handler.notify;

import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.SliceLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * 分片leader响应：集群leader通知分片leader已经选出新follow。通知接收处理
 */
public class NotifySliceLeaderVoteNewFollowHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();//新+旧
        String[] items=body.split("|");
        SliceLeaderService.syncDataToNewFollow(new Node(items[1]),new Node(items[0]));
        return null;
    }
}
