package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;

/**
 * leader响应：follow发送的心跳信息
 */
public class FollowHeartbeatToLeaderHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String address =frame.getSource();
        String body=frame.getBody();
        switch (body){
            case StringConstant.BROKER:
                RegBroker registerNode= LeaderService.BROKER_REGISTER_CENTER.get(address);
                if(registerNode==null){
                    BaseNode node=new BaseNode(address);
                    registerNode=new RegBroker(node);
                    LeaderService.BROKER_REGISTER_CENTER.put(address,registerNode);
                    LeaderService.notifyClinetsChangedBroker(registerNode.getAddress(),null, StringConstant.ADD);
                }else {
                    registerNode.setLastHeartbeat(ZonedDateTime.now());
                }
                break;
            case StringConstant.CLINET:
                RegClient clientNode= LeaderService.CLIENT_REGISTER_CENTER.get(address);
                if(clientNode==null){
                    BaseNode node=new BaseNode(address);
                    clientNode=new RegClient(node);
                    LeaderService.CLIENT_REGISTER_CENTER.put(address,clientNode);
                    LeaderService.notifyBrokersChangedClinet(clientNode.getAddress(), StringConstant.ADD);
                }else {
                    clientNode.setLastHeartbeat(ZonedDateTime.now());
                }
                break;
            default:break;
        }
        return null;
    }
}
