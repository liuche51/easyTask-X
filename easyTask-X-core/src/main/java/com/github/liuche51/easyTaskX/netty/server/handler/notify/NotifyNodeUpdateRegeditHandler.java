package com.github.liuche51.easyTaskX.netty.server.handler.notify;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * Broker响应leader通知当前节点更新注册表信息。
 * 异步执行
 */
public class NotifyNodeUpdateRegeditHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ClusterService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                BrokerService.requestUpdateRegedit();
            }
        });
        return null;
    }
}
