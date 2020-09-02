package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;

/**
 * 节点心跳信息处理
 */
public class HeartbeatHandler extends BaseHandler{
    @Override
    public String process(Dto.Frame frame) throws Exception {
        String follow =frame.getSource();
        NodeDto.Node node = NodeDto.Node.parseFrom(frame.getBodyBytes());
        Node node2=Node.parse(node);
        ClusterLeaderService.REGISTERCENTER.put(follow,node2);
        return null;
    }
}
