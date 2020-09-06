package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 更新Broker注册表信息
 */
public class UpdateBrokerRegeditHandler extends BaseHandler {
    @Override
    public String process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split("|");
        switch (items[0]){
            case "follow":
                RegisterNode node=ClusterLeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
                ConcurrentHashMap<String, Node> follows= JSONObject.parseObject(items[1],new TypeReference<ConcurrentHashMap<String, Node>>(){});
                node.getNode().setFollows(follows);
                break;
            default:break;
        }
        return null;
    }
}
