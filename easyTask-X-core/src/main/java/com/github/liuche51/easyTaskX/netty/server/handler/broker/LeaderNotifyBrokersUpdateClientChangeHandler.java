package com.github.liuche51.easyTaskX.netty.server.handler.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 *  Broker响应：leader通知brokers更新Client列表变动信息
 */
public class LeaderNotifyBrokersUpdateClientChangeHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_STRING);//type+地址
        switch (items[0]) {
            case StringConstant.ADD:
                NodeService.CURRENTNODE.getClients().add(new BaseNode(items[1]));
                break;
            case StringConstant.DELETE:
                Iterator<BaseNode> temps = NodeService.CURRENTNODE.getClients().iterator();
                while (temps.hasNext()) {
                    BaseNode bn = temps.next();
                    if (bn.getAddress().equals(items[1]))
                        NodeService.CURRENTNODE.getClients().remove(bn);
                }
                break;
            default:break;
        }
        return null;
    }
}
