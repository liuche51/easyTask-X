package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.BakLeaderRequestUpdateRegeditTask;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class BakLeaderService {
    private static final Logger log = LoggerFactory.getLogger(BakLeaderService.class);

    /**
     * BakLeader定时从leader获取注册表最新信息
     * 覆盖本地信息
     *
     * @return
     */
    public static void requestUpdateClusterRegedit() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BakLeaderRequestLeaderSendRegedit).setSource(NodeService.getConfig().getAddress());
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                String body = respPack.getRespbody().toStringUtf8();
                String[] items = body.split(StringConstant.CHAR_SPRIT_STRING);
                ConcurrentHashMap<String, RegBroker> broker = JSONObject.parseObject(items[0], new TypeReference<ConcurrentHashMap<String, RegBroker>>() {
                });
                ConcurrentHashMap<String, RegClient> clinet = JSONObject.parseObject(items[1], new TypeReference<ConcurrentHashMap<String, RegClient>>() {
                });
                LeaderService.BROKER_REGISTER_CENTER = broker;
                LeaderService.CLIENT_REGISTER_CENTER = clinet;
            } else {
                log.info("normally exception!requestUpdateClusterRegedit() failed.");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 启动BakLeader主动通过定时任务从leader更新注册表
     */
    public static TimerTask startBakLeaderRequestUpdateRegeditTask() {
        BakLeaderRequestUpdateRegeditTask task = new BakLeaderRequestUpdateRegeditTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }
}
