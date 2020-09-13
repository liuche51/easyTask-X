package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群leader响应Brokers更新注册表信息
 */
public class UpdateRegeditHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        switch (body){
            case "broker":
                RegisterNode node=ClusterLeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
                NodeDto.Node.Builder nodeBuilder=NodeDto.Node.newBuilder();
                //clients
                NodeDto.NodeList.Builder clientsBuilder= NodeDto.NodeList.newBuilder();
                Iterator<Map.Entry<String,Node>> items=node.getNode().getClients().entrySet().iterator();
                while (items.hasNext()){
                    Map.Entry<String,Node> item=items.next();
                    Node itNode=item.getValue();
                    NodeDto.Node.Builder clientBuilder= NodeDto.Node.newBuilder();
                    clientBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                    clientsBuilder.addNodes(clientBuilder.build());
                }
                nodeBuilder.setClients(clientsBuilder.build());
                //follows
                NodeDto.NodeList.Builder followsBuilder= NodeDto.NodeList.newBuilder();
                Iterator<Map.Entry<String,Node>> items2=node.getNode().getFollows().entrySet().iterator();
                while (items2.hasNext()){
                    Map.Entry<String,Node> item2=items2.next();
                    Node itNode=item2.getValue();
                    NodeDto.Node.Builder followBuilder= NodeDto.Node.newBuilder();
                    followBuilder.setHost(itNode.getHost()).setPort(itNode.getPort());
                    followsBuilder.addNodes(followBuilder.build());
                }
                nodeBuilder.setFollows(followsBuilder.build());
                //leaders
                NodeDto.NodeList.Builder leadersBuilder= NodeDto.NodeList.newBuilder();
                Iterator<Map.Entry<String,Node>> items3=node.getNode().getLeaders().entrySet().iterator();
                while (items3.hasNext()){
                    Map.Entry<String,Node> item3=items3.next();
                    Node itNode=item3.getValue();
                    NodeDto.Node.Builder followBuilder3= NodeDto.Node.newBuilder();
                    followBuilder3.setHost(itNode.getHost()).setPort(itNode.getPort());
                    leadersBuilder.addNodes(followBuilder3.build());
                }
                nodeBuilder.setLeaders(leadersBuilder.build());
                return nodeBuilder.build().toByteString();
            case "client":
                RegisterNode node1=ClusterLeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());

                break;

            default:break;
        }
        return null;
    }
}
