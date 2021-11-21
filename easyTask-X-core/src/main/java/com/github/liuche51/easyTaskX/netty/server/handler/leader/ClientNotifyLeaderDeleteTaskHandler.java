package com.github.liuche51.easyTaskX.netty.server.handler.leader;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Client用户删除任务通知
 * 1、同步方式删除。有异常就返回失败
 * 2、leader全局通缉方式删除任务
 */
public class ClientNotifyLeaderDeleteTaskHandler extends BaseHandler {
    /**
     * 全局通缉删除任务结果。
     * 1、只要一个节点异常或错误，整个结果都算出错
     */
    private boolean result = Boolean.TRUE;

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        StringListDto.StringList list = StringListDto.StringList.parseFrom(frame.getBodyBytes());
        List<String> taskIds = list.getListList();
        final CountDownLatch countDown = new CountDownLatch(LeaderService.BROKER_REGISTER_CENTER.size() + LeaderService.CLIENT_REGISTER_CENTER.size());
        Iterator<String> brokers = LeaderService.BROKER_REGISTER_CENTER.keySet().iterator();
        while (brokers.hasNext()) {
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        notifyFollow(taskIds, brokers.next());
                    } finally {
                        countDown.countDown();
                    }
                }
            });
        }
        Iterator<String> clients = LeaderService.CLIENT_REGISTER_CENTER.keySet().iterator();
        while (clients.hasNext()) {
            NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        notifyFollow(taskIds, clients.next());
                    } finally {
                        countDown.countDown();
                    }
                }
            });
        }
        boolean await = countDown.await(NodeService.getConfig().getAdvanceConfig().getTimeOut(), TimeUnit.SECONDS);
        if (!this.result || !await) { // 节点处理有错误或超时
            return ByteString.copyFromUtf8(StringConstant.FALSE);
        } else {
            return ByteString.copyFromUtf8(StringConstant.TRUE);
        }
    }

    private void notifyFollow(List<String> taskIds, String address) {
        try {
            StringListDto.StringList.Builder builder0 = StringListDto.StringList.newBuilder();
            taskIds.forEach(x -> {
                builder0.addList(x);
            });
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientNotifyLeaderDeleteTask).setSource(NodeService.getConfig().getAddress())
                    .setBodyBytes(builder0.build().toByteString());
            NettyClient client = new BaseNode(address).getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
            if (!ret) {// 偶发异常，重新入队列重发

            }
        } catch (Exception e) {
            this.result = Boolean.FALSE;
            log.error("", e);
        }
    }
}
