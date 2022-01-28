package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.Map;

/**
 * Broker通知leader修改注册节点的状态信息
 */
public class BrokerNotifyLeaderChangeRegNodeStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_COMMA);//节点类型+Map<状态属性名称,值>
        String type = items[0];
        Map<String, Integer> attr = JSONObject.parseObject(items[1], Map.class);
        String address = frame.getSource();
        RegNode regNode = null;
        switch (type) {
            case StringConstant.BROKER:
                regNode = LeaderService.BROKER_REGISTER_CENTER.get(address);
                break;
            case StringConstant.CLINET:
                regNode = LeaderService.CLIENT_REGISTER_CENTER.get(address);
                break;
            default:
                break;
        }
        if (regNode != null) {
            Iterator<Map.Entry<String, Integer>> items2 = attr.entrySet().iterator();
            while (items2.hasNext()) {
                Map.Entry<String, Integer> item = items2.next();
                switch (item.getKey()) {
                    case StringConstant.DATASTATUS:
                        regNode.setDataStatus(item.getValue());
                        break;
                    case StringConstant.NODESTATUS:
                        regNode.setNodeStatus(item.getValue());
                        break;
                    default:
                        break;
                }
            }

        }
        return null;
    }
}
