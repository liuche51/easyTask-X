package com.github.liuche51.easyTaskX.cluster.follow;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;

import java.util.concurrent.ConcurrentHashMap;

public class BrokerUtil {
    /**
     * 更新slave节点的MasterBinlogInfo信息
     * 1、将新master加入到同步master集合
     * 2、将失效的master移除掉。
     * @param masters
     */
    public static void updateMasterBinlogInfo(ConcurrentHashMap<String, BaseNode> masters){
        //获取新加入的master节点
        masters.keySet().forEach(x->{
            if(!SlaveService.MASTER_SYNC_BINLOG_INFO.contains(x)){
                SlaveService.MASTER_SYNC_BINLOG_INFO.put(x,new MasterNode(x));
            }
        });
        //删除已经失效的master
        SlaveService.MASTER_SYNC_BINLOG_INFO.keySet().forEach(x->{
            if(!masters.contains(x)){
                SlaveService.MASTER_SYNC_BINLOG_INFO.remove(x);
            }
        });

    }
}
