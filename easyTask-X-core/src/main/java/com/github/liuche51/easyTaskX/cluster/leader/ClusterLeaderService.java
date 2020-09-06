package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.dto.RegisterNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    public static List<String> getRegisteredBokers(){
        Iterator<Map.Entry<String, RegisterNode>> items=BROKER_REGISTER_CENTER.entrySet().iterator();
        List<String> list=new ArrayList<>(BROKER_REGISTER_CENTER.size());
        while (items.hasNext()){
            list.add(items.next().getKey());
        }
        return list;
    }
}
