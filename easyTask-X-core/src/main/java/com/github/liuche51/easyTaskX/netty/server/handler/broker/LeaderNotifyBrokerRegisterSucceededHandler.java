package com.github.liuche51.easyTaskX.netty.server.handler.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * leader通知Broker注册成功。
 * 1、接口不用考虑幂等性。因为NodeService.initCURRENT_NODE()可以被重复调用
 * 2、Broker被注册成功，有两种情况。第一种是节点重新启动，第二种是因为网络问题导致心跳信息失联，leader判定节点失效，被踢出集群。随后网络恢复，心跳继续导致被重新注册。
 * 3、节点因网络问题被重新注册，需要重新初始化系统，以新节点方式加入集群
 */
public class LeaderNotifyBrokerRegisterSucceededHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        //判断是否是首次注册成功，是，则不需要重复初始化。否，就表示可以重新初始化。
        if (!NodeService.IS_FIRST_STARTED) {
            NodeService.initCURRENT_NODE(false);
        }
        NodeService.IS_FIRST_STARTED = false;
        return null;
    }
}

