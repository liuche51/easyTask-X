package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.Map;

/**
 * leader响应Brokers更新注册表信息
 */
public class UpdateRegeditHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        switch (body){
            case "broker":
                RegBroker node= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
                NodeDto.Node.Builder nodeBuilder=NodeDto.Node.newBuilder();
                //clients
                NodeDto.NodeList.Builder clientsBuilder= NodeDto.NodeList.newBuilder();
                Iterator<Map.Entry<String,RegNode>> items=node.getClients().entrySet().iterator();
                while (items.hasNext()){
                    Map.Entry<String, RegNode> item=items.next();
                    RegNode itNode=item.getValue();
                    NodeDto.Node.Builder clientBuilder= NodeDto.Node.newBuilder();
                    clientBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                    clientsBuilder.addNodes(clientBuilder.build());
                }
                nodeBuilder.setClients(clientsBuilder.build());
                //follows
                NodeDto.NodeList.Builder followsBuilder= NodeDto.NodeList.newBuilder();
                Iterator<Map.Entry<String,RegNode>> items2=node.getFollows().entrySet().iterator();
                while (items2.hasNext()){
                    Map.Entry<String,RegNode> item2=items2.next();
                    RegNode itNode=item2.getValue();
                    NodeDto.Node.Builder followBuilder= NodeDto.Node.newBuilder();
                    followBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                    followsBuilder.addNodes(followBuilder.build());
                }
                nodeBuilder.setFollows(followsBuilder.build());
                //leaders
                NodeDto.NodeList.Builder leadersBuilder= NodeDto.NodeList.newBuilder();
                Iterator<Map.Entry<String,RegNode>> items3=node.getLeaders().entrySet().iterator();
                while (items3.hasNext()){
                    Map.Entry<String,RegNode> item3=items3.next();
                    RegNode itNode=item3.getValue();
                    NodeDto.Node.Builder followBuilder3= NodeDto.Node.newBuilder();
                    followBuilder3.setHost(itNode.getHost()).setPort(itNode.getPort());
                    leadersBuilder.addNodes(followBuilder3.build());
                }
                nodeBuilder.setLeaders(leadersBuilder.build());
                return nodeBuilder.build().toByteString();
            case "client":
                RegBroker node1= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());

                break;

            default:break;
        }
        return null;
    }
}
