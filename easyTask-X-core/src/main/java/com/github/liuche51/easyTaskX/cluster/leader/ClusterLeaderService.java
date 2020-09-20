package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.task.CheckFollowsAliveTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterLeaderService {
    private static final Logger log = LoggerFactory.getLogger(VoteSliceFollows.class);
    /**
     * 集群BROKER注册表
     */
    public static ConcurrentHashMap<String, RegisterNode> BROKER_REGISTER_CENTER = new ConcurrentHashMap<>(10);
    /**
     * 集群CLIENT注册表
     */
    public static ConcurrentHashMap<String, RegisterNode> CLIENT_REGISTER_CENTER = new ConcurrentHashMap<>(10);

    /**
     * 通知节点更新注册表信息
     *
     * @param nodes
     */
    public static void notifyNodeUpdateRegedit(List<Node> nodes) {

        nodes.forEach(x -> {
            ClusterService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.NOTIFY_NODE_UPDATE_REGEDIT)
                                .setSource(ClusterService.CURRENTNODE.getAddress());
                        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, x.getClient(), ClusterService.getConfig().getAdvanceConfig().getTryCount(), 5,null);
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        });
    }

    /**
     * 集群follow请求集群leader获取当前节点最新注册表信息。
     * 覆盖本地信息
     *
     * @return
     */
    public static boolean requestUpdateRegedit() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.UPDATE_REGEDIT).setSource(ClusterService.getConfig().getAddress())
            .setBody("broker");
            ByteStringPack respPack =new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, ClusterService.CURRENTNODE.getClusterLeader().getClient(), ClusterService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                NodeDto.Node node = NodeDto.Node.parseFrom(respPack.getRespbody());
                NodeDto.NodeList clientNodes = node.getClients();
                ConcurrentHashMap<String, Node> clients = new ConcurrentHashMap<>();
                clientNodes.getNodesList().forEach(x -> {
                    clients.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
                });
                NodeDto.NodeList followNodes = node.getFollows();
                ConcurrentHashMap<String, Node> follows = new ConcurrentHashMap<>();
                followNodes.getNodesList().forEach(x -> {
                    follows.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
                });
                NodeDto.NodeList leaderNodes = node.getLeaders();
                ConcurrentHashMap<String, Node> leaders = new ConcurrentHashMap<>();
                leaderNodes.getNodesList().forEach(x -> {
                    leaders.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
                });
                ClusterService.CURRENTNODE.setFollows(follows);
                ClusterService.CURRENTNODE.setClients(clients);
                ClusterService.CURRENTNODE.setLeaders(leaders);
                ClusterService.CURRENTNODE.setLastHeartbeat(ZonedDateTime.now());
                return true;
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }
    /**
     * 启动集群leader检查所有follows是否存活任务
     */
    public static TimerTask startCheckFollowAliveTask() {
        CheckFollowsAliveTask task=new CheckFollowsAliveTask();
        task.start();
        return task;
    }
}
