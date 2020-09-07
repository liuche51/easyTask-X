package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.google.protobuf.ByteString;

/**
 * 集群leader通知当前节点更新注册表信息。
 * 异步执行
 */
public class NotifyNodeUpdateRegeditHandler extends BaseHandler{

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ClusterService.getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                ClusterLeaderService.requestUpdateRegedit(3,5);
            }
        });
        return null;
    }
}
