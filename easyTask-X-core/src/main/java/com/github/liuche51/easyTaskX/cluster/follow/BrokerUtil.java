package com.github.liuche51.easyTaskX.cluster.follow;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;

import java.util.concurrent.ConcurrentHashMap;

public class BrokerUtil {
    /**
     * 处理注册表更新
     *
     * @param node
     */
    public static void dealUpdate(NodeDto.Node node) {
        NodeDto.NodeList slaveNodes = node.getSalves();
        ConcurrentHashMap<String, BaseNode> follows = new ConcurrentHashMap<>();
        slaveNodes.getNodesList().forEach(x -> {
            follows.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeDto.NodeList masterNodes = node.getMasters();
        ConcurrentHashMap<String, BaseNode> leaders = new ConcurrentHashMap<>();
        masterNodes.getNodesList().forEach(x -> {
            leaders.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeService.CURRENTNODE.setSlaves(follows);
        NodeService.CURRENTNODE.setMasters(leaders);
    }
}
