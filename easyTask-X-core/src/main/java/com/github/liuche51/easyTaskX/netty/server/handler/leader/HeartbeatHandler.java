package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;

/**
 * 节点注册/心跳信息处理
 */
public class HeartbeatHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String address =frame.getSource();
        String body=frame.getBody();
        switch (body){
            case "Broker":
                RegBroker registerNode= LeaderService.BROKER_REGISTER_CENTER.get(address);
                if(registerNode==null){
                    Node node=new Node(address);
                    registerNode=new RegBroker(node);
                    LeaderService.BROKER_REGISTER_CENTER.put(address,registerNode);
                }else {
                    registerNode.setLastHeartbeat(ZonedDateTime.now());
                }
                break;
            case "Client":
                RegClient clientNode= LeaderService.CLIENT_REGISTER_CENTER.get(address);
                if(clientNode==null){
                    Node node=new Node(address);
                    clientNode=new RegClient(node);
                    LeaderService.CLIENT_REGISTER_CENTER.put(address,clientNode);
                }else {
                    clientNode.setLastHeartbeat(ZonedDateTime.now());
                }
                break;
            default:break;
        }
        return null;
    }
}
