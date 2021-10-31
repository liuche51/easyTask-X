package com.github.liuche51.easyTaskX.netty.server.handler.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * leader通知Broker注册成功。
 * 接口不用考虑幂等性。因为NodeService.initCURRENT_NODE()可以被重复调用
 */
public class LeaderNotifyBrokerRegisterSucceededHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        //判断是否是首次注册成功，是，则不需要重复初始化。否，就表示可以重新初始化。
        if (!NodeService.isFirstStarted) {
            NodeService.initCURRENT_NODE();
        }
        NodeService.isFirstStarted = false;
        return null;
    }
}

