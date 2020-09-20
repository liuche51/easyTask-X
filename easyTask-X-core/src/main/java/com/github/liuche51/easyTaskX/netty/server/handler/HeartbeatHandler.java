package com.github.liuche51.easyTaskX.netty.server.handler;

import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;

/**
 * 节点心跳信息处理
 */
public class HeartbeatHandler extends BaseHandler{
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String address =frame.getSource();
        String body=frame.getBody();
        switch (body){
            case "Broker":
                RegBroker registerNode=ClusterLeaderService.BROKER_REGISTER_CENTER.get(address);
                if(registerNode==null){
                    Node node=new Node(address);
                    registerNode=new RegBroker(node);
                    ClusterLeaderService.BROKER_REGISTER_CENTER.put(address,registerNode);
                }else {
                    registerNode.setLastHeartbeat(ZonedDateTime.now());
                }
                break;
            case "Client":
                RegBroker clientNode=ClusterLeaderService.CLIENT_REGISTER_CENTER.get(address);
                if(clientNode==null){
                    Node node=new Node(address);
                    clientNode=new RegBroker(node);
                    ClusterLeaderService.CLIENT_REGISTER_CENTER.put(address,clientNode);
                }else {
                    clientNode.setLastHeartbeat(ZonedDateTime.now());
                }
                break;
            default:break;
        }
        return null;
    }
}
