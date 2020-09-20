package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Node;

import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteClusterLeader;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;
import io.netty.channel.ChannelFuture;

/**
 * 节点对集群leader的心跳。
 */
public class HeartbeatsTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Node leader = ClusterService.CURRENTNODE.getClusterLeader();
                if (leader == null) {//启动时还没获取集群leader信息，所以需要去zk获取
                    LeaderData node = ZKService.getClusterLeaderData();
                    if (node != null && !StringUtils.isNullOrEmpty(node.getHost())) {//获取集群leader信息成功
                        leader = new Node(node.getHost(), node.getPort());
                        ClusterService.CURRENTNODE.setClusterLeader(leader);
                    } else {//否则就进入选举
                        VoteClusterLeader.competeLeader();
                    }
                } else {
                    dealClusterLeaderCheckFollowsAliveTask(leader);
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.Heartbeat).setSource(ClusterService.getConfig().getAddress())
                            .setBody("Broker");//服务端节点
                    ChannelFuture future = NettyMsgService.sendASyncMsg(leader.getClient(), builder.build());//这里使用异步即可。也不需要返回值
                }
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getAdvanceConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    /**
     * 处理集群leader，是否运行检查follows存活任务的逻辑
     * @param leader
     */
    private static void dealClusterLeaderCheckFollowsAliveTask(Node leader) {
        if (leader == null) return;
        //如果当前节点是集群leader，且没有运行follow存活检查任务，则启动一个任务/
        if (!CheckFollowsAliveTask.hasRuning && ClusterService.CURRENTNODE.getAddress().equals(leader.getAddress())) {
            ClusterService.timerTasks.add(ClusterLeaderService.startCheckFollowAliveTask());
        }
        if (!ClusterService.CURRENTNODE.getAddress().equals(leader.getAddress()) && CheckFollowsAliveTask.hasRuning) {
            CheckFollowsAliveTask.hasRuning = false;
        }
    }

}
