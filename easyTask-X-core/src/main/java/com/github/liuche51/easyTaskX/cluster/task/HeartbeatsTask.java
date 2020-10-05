package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteLeader;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;
import io.netty.channel.ChannelFuture;

/**
 * 节点对leader的心跳。
 */
public class HeartbeatsTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                BaseNode leader = NodeService.CURRENTNODE.getClusterLeader();
                if (leader == null) {//启动时还没获取leader信息，所以需要去zk获取
                    LeaderData node = ZKService.getLeaderData(false);
                    if (node != null && !StringUtils.isNullOrEmpty(node.getHost())) {//获取leader信息成功
                        leader = new BaseNode(node.getHost(), node.getPort());
                        NodeService.CURRENTNODE.setClusterLeader(leader);
                    }
                    //如果当前备用leader信息是空的，说明是集群首次运行。则每个节点都可以进入选举.
                    //或者自身是备用leader，则有权去竞选新leader
                    else if(StringUtils.isNullOrEmpty(NodeService.CURRENTNODE.getBakLeader())
                            ||NodeService.CURRENTNODE.getBakLeader().contains(NodeService.CURRENTNODE.getAddress())){
                        VoteLeader.competeLeader();
                    }
                } else {
                    initLeaderCheckFollowsAliveTask(leader);
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.Heartbeat).setSource(NodeService.getConfig().getAddress())
                            .setBody("Broker");//服务端节点
                    ChannelFuture future = NettyMsgService.sendASyncMsg(leader.getClient(), builder.build());//这里使用异步即可。也不需要返回值
                }
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                Thread.sleep(NodeService.getConfig().getAdvanceConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    /**
     * 初始化是否运行leader的检查follows存活任务
     * @param leader
     */
    private static void initLeaderCheckFollowsAliveTask(BaseNode leader) {
        if (leader == null) return;
        //如果当前节点是leader，且没有运行follow存活检查任务，则启动一个任务/
        if (!CheckFollowsAliveTask.hasRuning && NodeService.CURRENTNODE.getAddress().equals(leader.getAddress())) {
            NodeService.timerTasks.add(LeaderService.startCheckFollowAliveTask());
        }
        if (!NodeService.CURRENTNODE.getAddress().equals(leader.getAddress()) && CheckFollowsAliveTask.hasRuning) {
            CheckFollowsAliveTask.hasRuning = false;
        }
    }

}
