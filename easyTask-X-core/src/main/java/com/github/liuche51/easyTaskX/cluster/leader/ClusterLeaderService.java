package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.dto.RegisterNode;

import java.util.concurrent.ConcurrentHashMap;

public class ClusterLeaderService {
    /**
     * 集群NODE注册表
     */
    public static ConcurrentHashMap<String, RegisterNode> BROKER_REGISTER_CENTER=new ConcurrentHashMap<>(10);
    /**
     * 集群CLIENT注册表
     */
    public static ConcurrentHashMap<String, RegisterNode> CLIENT_REGISTER_CENTER=new ConcurrentHashMap<>(10);
}
