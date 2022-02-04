package com.github.liuche51.easyTaskX.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.LogErrorUtil;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerUtil {

    /**
     * 更新slave节点的MasterBinlogInfo信息
     * 1、将新master加入到同步master集合
     * 2、将失效的master移除掉。
     *
     * @param masters
     */
    public static void updateMasterBinlogInfo(ConcurrentHashMap<String, BaseNode> masters) {
        //获取新加入的master节点
        masters.keySet().forEach(x -> {
            if (!SlaveService.MASTER_SYNC_BINLOG_INFO.contains(x)) {
                SlaveService.MASTER_SYNC_BINLOG_INFO.put(x, new MasterNode(x));
            }
        });
        //删除已经失效的master
        SlaveService.MASTER_SYNC_BINLOG_INFO.keySet().forEach(x -> {
            if (!masters.contains(x)) {
                SlaveService.MASTER_SYNC_BINLOG_INFO.remove(x);
            }
        });

    }

    /**
     * Broker通知leader修改注册节点的状态信息
     * 覆盖本地信息
     *
     * @return
     */
    public static void notifyLeaderChangeRegNodeStatus(Map<String,Integer> attr) {
        BrokerService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BrokerNotifyLeaderChangeRegNodeStatus).setSource(BrokerService.getConfig().getAddress())
                            .setBody(StringConstant.BROKER+StringConstant.CHAR_SPRIT_COMMA+ JSONObject.toJSONString(attr));
                    ByteStringPack respPack = new ByteStringPack();
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, BrokerService.CLUSTER_LEADER.getClient(), BrokerService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
                    if (!ret) {
                        LogErrorUtil.writeRpcErrorMsgToDb("Broker通知leader修改注册节点的状态信息。失败！", "com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil.notifyLeaderChangeRegNodeStatus");
                    }
                } catch (Exception e) {
                    LogUtil.error("", e);
                }
            }
        });
    }
}
