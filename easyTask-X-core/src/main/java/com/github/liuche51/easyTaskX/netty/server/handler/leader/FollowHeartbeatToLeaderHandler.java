package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.enume.RegNodeTypeEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

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
                    LeaderService.notifyBrokerRegisterSucceeded(node);
                    LeaderService.notifyClinetsChangedBroker(registerNode.getAddress(),null, OperationTypeEnum.ADD);
                }else {
                    registerNode.setLastHeartbeat(ZonedDateTime.now());
                    LeaderService.addFollowsHeartbeats(address,RegNodeTypeEnum.REGBROKER);
                }
                break;
            case StringConstant.CLINET:
                RegClient clientNode= LeaderService.CLIENT_REGISTER_CENTER.get(address);
                if(clientNode==null){
                    BaseNode node=new BaseNode(address);
                    clientNode=new RegClient(node);
                    LeaderService.CLIENT_REGISTER_CENTER.put(address,clientNode);
                    LeaderService.notifyBrokersChangedClinet(clientNode.getAddress(), OperationTypeEnum.ADD);
                    List<BinlogClusterMeta> binlogClusterMetas=new ArrayList<>(1);
                    binlogClusterMetas.add(new BinlogClusterMeta(OperationTypeEnum.ADD, RegNodeTypeEnum.REGCLIENT,address, JSONObject.toJSONString(clientNode)));
                }else {
                    clientNode.setLastHeartbeat(ZonedDateTime.now());
                    LeaderService.addFollowsHeartbeats(address,RegNodeTypeEnum.REGCLIENT);
                }
                break;
            default:break;
        }
        return null;
    }
}
