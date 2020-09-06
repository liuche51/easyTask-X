package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;

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
public class HeartbeatsTask extends TimerTask{
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Node leader=ClusterService.CURRENTNODE.getClusterLeader();
                if(leader==null){//启动时还没获取集群leader信息，所以需要去zk获取
                    LeaderData node=ZKService.getClusterLeaderData();
                    if(node!=null&& !StringUtils.isNullOrEmpty(node.getHost())){//获取集群leader信息成功
                        leader=new Node(node.getHost(),node.getPort());
                        ClusterService.CURRENTNODE.setClusterLeader(leader);
                    }else {//否则就进入选举
                        VoteClusterLeader.competeLeader();
                    }
                }else {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FOLLOW_TO_LEADER_HEARTBEAT).setSource(ClusterService.getConfig().getAddress())
                            .setBody("NODE");//服务端节点
                    ChannelFuture future = NettyMsgService.sendASyncMsg(leader.getClient(), builder.build());//这里使用异步即可。也不需要返回值
                }
            } catch (Exception e) {
                log.error("",e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("",e);
            }
        }
    }

}
