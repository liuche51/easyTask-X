package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.leader.CheckFollowsAliveTask;
import com.github.liuche51.easyTaskX.cluster.task.leader.ClusterMetaBinLogSyncTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteLeader;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;
import io.netty.channel.ChannelFuture;

import java.util.concurrent.TimeUnit;

/**
 * Follow节点对leader的心跳。
 */
public class HeartbeatsTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                BaseNode leader = BrokerService.CLUSTER_LEADER;
                if (leader == null) {//启动时还没获取leader信息，所以需要去zk获取
                    LeaderData node = ZKService.getLeaderData(false);
                    if (node != null && !StringUtils.isNullOrEmpty(node.getHost())) {//获取leader信息成功
                        leader = new BaseNode(node.getHost(), node.getPort());
                        BrokerService.CLUSTER_LEADER=leader;
                    }
                    //如果当前备用leader信息是空的，说明是集群首次运行。则每个节点都可以进入选举.
                    //或者自身是备用leader，则有权去竞选新leader
                    else if (StringUtils.isNullOrEmpty(BrokerService.BAK_LEADER)
                            || BrokerService.BAK_LEADER.contains(BrokerService.CURRENT_NODE.getAddress())) {
                        VoteLeader.competeLeader();
                    }
                } else {
                    enableOrdisableLeaderCheckFollowsAliveTask(leader);
                    enableOrdisableClusterMetaBinLogSyncTask(leader);
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowHeartbeatToLeader).setSource(BrokerService.getConfig().getAddress())
                            .setBody(StringConstant.BROKER);//服务端节点
                    ChannelFuture future = NettyMsgService.sendASyncMsg(leader.getClient(), builder.build());//这里使用异步即可。也不需要返回值
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                TimeUnit.SECONDS.sleep(BrokerService.getConfig().getAdvanceConfig().getHeartBeat());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }

    /**
     * 启用或关闭leader的检查follows存活任务
     *
     * @param leader
     */
    private static void enableOrdisableLeaderCheckFollowsAliveTask(BaseNode leader) {
        if (leader == null) return;
        //如果当前节点是leader，且没有运行follow存活检查任务，则启动一个任务/
        if (BrokerService.CURRENT_NODE.getAddress().equals(leader.getAddress()) && !CheckFollowsAliveTask.hasRuning) {
            BrokerService.timerTasks.add(LeaderService.startCheckFollowAliveTask());
        }
        //如果当前节点不是leader，且任务在运行中，则关闭。
        if (!BrokerService.CURRENT_NODE.getAddress().equals(leader.getAddress()) && CheckFollowsAliveTask.hasRuning) {
            CheckFollowsAliveTask.hasRuning = false;
        }
    }

    /**
     * 启用或关闭bakleader对leader的心跳binlog任务
     */
    private static void enableOrdisableClusterMetaBinLogSyncTask(BaseNode leader) {
        //如果当前节点是bakleader，且不是leader，且没有运行对leader的心跳binlog任务，则启动一个任务/
        if (BrokerService.BAK_LEADER.contains(BrokerService.CURRENT_NODE.getAddress())&&!BrokerService.CURRENT_NODE.getAddress().equals(leader.getAddress()) && !ClusterMetaBinLogSyncTask.hasRuning) {
            BrokerService.timerTasks.add(LeaderService.startCheckFollowAliveTask());
        }
        //如果当前节点不是bakleader，且也不是leader，且任务在运行中，则立即停止。防止当前节点不再是bakleader或leader还保存有
        if (!BrokerService.BAK_LEADER.contains(BrokerService.CURRENT_NODE.getAddress())&&!BrokerService.CURRENT_NODE.getAddress().equals(leader.getAddress()) && CheckFollowsAliveTask.hasRuning) {
            ClusterMetaBinLogSyncTask.hasRuning = false;
            LeaderService.BROKER_REGISTER_CENTER.clear();
            LeaderService.CLIENT_REGISTER_CENTER.clear();
        }
    }
}
