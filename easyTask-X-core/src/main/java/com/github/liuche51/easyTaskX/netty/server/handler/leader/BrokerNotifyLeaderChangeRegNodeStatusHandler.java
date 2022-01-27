package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * Broker通知leader修改注册节点的状态信息
 */
public class BrokerNotifyLeaderChangeRegNodeStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_COMMA);//节点类型+状态属性名称+值
        String type = items[0], arrt = items[1], value = items[2];
        String address = frame.getSource();
        RegNode regNode = null;
        switch (type) {
            case StringConstant.BROKER:
                regNode = LeaderService.BROKER_REGISTER_CENTER.get(address);
            case StringConstant.CLINET:
                regNode = LeaderService.CLIENT_REGISTER_CENTER.get(address);
            default:
                break;
        }
        if (regNode != null) {
            switch (arrt) {
                case "dataStatus":
                    regNode.setDataStatus(Integer.valueOf(value));
                    break;
                case "nodeStatus":
                    regNode.setNodeStatus(Integer.valueOf(value));
                    break;
                default:
                    break;
            }
        }
        return null;
    }
}
