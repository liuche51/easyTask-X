package com.github.liuche51.easyTaskX.netty.server.handler.notify;

import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * 集群leader响应：分片leader通知集群leader，已经完成对新follow的数据同步。请求更新数据同步状态
 */
public class NotifyClusterLeaderUpdateRegeditForDataStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split("|");
        RegisterNode regNode=ClusterLeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
        Node follow=regNode.getNode().getFollows().get(items[0]);
        follow.setDataStatus(new Short(items[1]));
        return null;
    }
}
