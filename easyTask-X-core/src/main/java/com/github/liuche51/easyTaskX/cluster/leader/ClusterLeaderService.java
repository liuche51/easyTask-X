package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.Node;

import java.util.concurrent.ConcurrentHashMap;

public class ClusterLeaderService {
    /**
     * 集群Follow注册表
     */
    public static ConcurrentHashMap<String, Node> REGISTERCENTER=new ConcurrentHashMap<>(10);
}
