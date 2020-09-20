package com.github.liuche51.easyTaskX.cluster.follow;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSliceFollows;
import com.github.liuche51.easyTaskX.cluster.task.HeartbeatsTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.UpdateRegeditTask;
import com.github.liuche51.easyTaskX.cluster.task.tran.*;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker
 */
public class BrokerService {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);
    /**
     * 启动批量事务数据提交任务
     */
    public static TimerTask startCommitSaveTransactionTask() {
        CommitSaveTransactionTask task=new CommitSaveTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动批量事务数据删除任务
     */
    public static TimerTask startCommitDelTransactionTask() {
        CommitDelTransactionTask task=new CommitDelTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动批量事务数据取消提交任务
     */
    public static TimerTask startCancelSaveTransactionTask() {
        CancelSaveTransactionTask task=new CancelSaveTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动重试取消保持任务
     */
    public static TimerTask startRetryCancelSaveTransactionTask() {
        RetryCancelSaveTransactionTask task=new RetryCancelSaveTransactionTask();
        task.start();
        return task;
    }
    /**
     * 启动重试删除任务
     */
    public static TimerTask startRetryDelTransactionTask() {
        RetryDelTransactionTask task=new RetryDelTransactionTask();
        task.start();
        return task;
    }
    /**
     * 节点对集群leader的心跳。2s一次
     */
    public static TimerTask startHeartBeat() {
        HeartbeatsTask task=new HeartbeatsTask();
        task.start();
        return task;
    }
    /**
     * 启动点定时从集群leader获取注册表更新任务
     */
    public static TimerTask startUpdateRegeditTask() {
        UpdateRegeditTask task=new UpdateRegeditTask();
        task.start();
        return task;
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
                return true;
            }else {
                log.info("normally exception!requestUpdateRegedit() failed.");
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }
}
