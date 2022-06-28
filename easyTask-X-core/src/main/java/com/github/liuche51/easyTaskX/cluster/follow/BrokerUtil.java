package com.github.liuche51.easyTaskX.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.HistoryScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.db.HistorySchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.*;
import com.github.liuche51.easyTaskX.zk.ZKService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BrokerUtil {
    /**
     * 清空所有表的记录
     * 节点宕机后，重启。或失去联系zk后又重新连接了。都视为新节点加入集群。加入前需要清空所有记录，避免有重复数据在集群中
     */
    public static void clearAllData() {
        try {
            while (true) {
                List<Schedule> scheduleList = ScheduleDao.selectByCount(1000);
                if (scheduleList.size() > 0) {
                    List<HistorySchedule> historySchedules = new ArrayList<>(scheduleList.size());
                    List<String> ids = new ArrayList<>(scheduleList.size());
                    scheduleList.forEach(x -> {
                        ids.add(x.getId());
                        historySchedules.add(new HistorySchedule(x.getId(), JSONObject.toJSONString(x)));
                    });
                    HistoryScheduleDao.saveBatch(historySchedules);
                    BinlogScheduleDao.deleteAll();
                    ScheduleDao.deleteByIds(ids.toArray(new String[]{}));
                } else {
                    break;
                }

            }
            BinlogScheduleDao.deleteAll();
            ScheduleBakDao.deleteAll();
        } catch (Exception e) {
            LogUtil.error("deleteAllData exception!", e);
        }
    }

    /**
     * 询问leader是否自己还处于存活状态
     * 1、如果自己因为重启，还处于心跳周期内。则leader认为还处于存活状态。这样就不用重新以新节点方式加入集群。也不用删除旧数据了。
     * @return
     */
    public static boolean isAliveInCluster() {
        try {
            LeaderData node = ZKService.getLeaderData(false);
            if (node != null && !StringUtils.isNullOrEmpty(node.getHost())) {//获取leader信息成功
                BaseNode leader = new BaseNode(node.getHost(), node.getPort());
                BrokerService.CLUSTER_LEADER=leader;
                Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowNotifyLeaderHasRestart)
                        .setSource(StringConstant.BROKER);
                ByteStringPack pack = new ByteStringPack();
                boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, leader.getClient(), BrokerService.getConfig().getAdvanceConfig().getTryCount(), 5, pack);
                if (!ret) {
                    ImportantErrorLogUtil.writeRpcErrorMsgToDb("Broker因重启询问leader是否自己还处于存活状态。失败！", "com.github.liuche51.easyTaskX.cluster.NodeUtil.isAliveInCluster");
                } else {
                    String result = pack.getRespbody().toStringUtf8();
                    if (StringConstant.ALIVE.equals(result)) {
                        return true;
                    }
                }
            }

        } catch (Exception e) {
            LogUtil.error("", e);
        }
        return false;
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
                        ImportantErrorLogUtil.writeRpcErrorMsgToDb("Broker通知leader修改注册节点的状态信息。失败！", "com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil.notifyLeaderChangeRegNodeStatus");
                    }
                } catch (Exception e) {
                    LogUtil.error("", e);
                }
            }
        });
    }
}
