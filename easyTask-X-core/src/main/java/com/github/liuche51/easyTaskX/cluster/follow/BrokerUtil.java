package com.github.liuche51.easyTaskX.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.ImportantErrorLogUtil;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.Map;

public class BrokerUtil {


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
                        ImportantErrorLogUtil.writeRpcErrorMsgToDb("Broker通知leader修改注册节点的状态信息。失败！", "com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil.notifyLeaderChangeRegNodeStatus");
                    }
                } catch (Exception e) {
                    LogUtil.error("", e);
                }
            }
        });
    }
}
