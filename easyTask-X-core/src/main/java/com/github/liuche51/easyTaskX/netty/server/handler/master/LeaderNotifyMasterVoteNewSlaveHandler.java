package com.github.liuche51.easyTaskX.netty.server.handler.master;

import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * master响应：leader通知master已经选出新follow。通知接收处理
 */
public class LeaderNotifyMasterVoteNewSlaveHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();//新+旧
        String[] items=body.split(StringConstant.CHAR_SPRIT_STRING);
        MasterService.syncDataToNewFollow(new Node(items[1]),new Node(items[0]));
        return null;
    }
}
