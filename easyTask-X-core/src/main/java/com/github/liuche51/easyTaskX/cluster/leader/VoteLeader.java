package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.ImportantErrorLogUtil;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;
import com.github.liuche51.easyTaskX.zk.ZKUtil;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 选举leader
 * 采用分布式锁的方式实现
 */
public class VoteLeader {
    private static InterProcessMutex zkMutex = new InterProcessMutex(ZKUtil.getClient(), "/mutex");

    public static boolean competeLeader() {
        LogUtil.info("leader 竞选开始!");
        //如果自己元数据同步还没有达到已完成状态。则需要查询其他bakleader是否有已完成的。如果有则自己等待3秒后再竞选，否则自己就可以直接去竞选了。
        //这样有利于选出拥有最新元数据的bakleader。比如一个刚刚加入的bakleader此时元数据还处于同步状态，理论上不应该作为新leader候选人
        if (BakLeaderService.DATA_STATUS.equals(0)) {
            if (haveOtherBakLeaderFinishedSync()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(3000L);
                } catch (InterruptedException e) {
                    LogUtil.error("", e);
                }
            }
        }
        boolean hasLock = false;
        try {
            if (zkMutex.acquire(1, TimeUnit.SECONDS)) {
                hasLock = true;
                LeaderData data = ZKService.getLeaderData(false);
                if (data == null || StringUtils.isNullOrEmpty(data.getHost())) {//leader节点为空时才需要选新leader
                    ZKService.registerLeader(new LeaderData(BrokerService.CURRENT_NODE.getHost(), BrokerService.CURRENT_NODE.getPort()));
                    return true;
                }

            }
        } catch (Exception e) {
            LogUtil.error("", e);
        } finally {
            if (hasLock) {
                try {
                    zkMutex.release();//释放锁，会删除mutex节点下的子节点（参与竞争的节点信息），所以你可能看不到，因为存续时间非常短
                } catch (Exception e) {
                    LogUtil.error("", e);
                }
            }

        }
        return false;
    }

    /**
     * 查询其他bakleader是否存在已经处于元数据同步状态的
     *
     * @return
     */
    private static boolean haveOtherBakLeaderFinishedSync() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BakLeaderQueryOtherBakLeaderDataStatus).setSource(BrokerService.getConfig().getAddress());
            String bakLeader = BrokerService.BAK_LEADER;
            if (!StringUtils.isNullOrEmpty(bakLeader)) {
                Map<String, BaseNode> bakLeaders = JSONObject.parseObject(bakLeader, new TypeReference<Map<String, BaseNode>>() {
                });
                for (BaseNode bak : bakLeaders.values()) {
                    if (bak.getAddress().equals(BrokerService.CURRENT_NODE.getAddress()))//排除查询自己
                        continue;
                    ByteStringPack respPack = new ByteStringPack();
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, bak.getClient(), BrokerService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
                    if (ret) {
                        if ("1".equals(respPack.getRespbody().toString())) {
                            return true;
                        }
                    } else {
                        ImportantErrorLogUtil.writeRpcErrorMsgToDb("Leader通知Broker注册成功。失败！", "com.github.liuche51.easyTaskX.cluster.leader.VoteLeader.haveOtherBakLeaderFinishedSync");
                    }
                }
            }
        } catch (Exception e) {
            LogUtil.error("", e);
        }
        return false;
    }
}
