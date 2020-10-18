package com.github.liuche51.easyTaskX.cluster.follow;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerUpdateClientsTask;
import com.github.liuche51.easyTaskX.cluster.task.follow.HeartbeatsTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.follow.FollowRequestUpdateRegeditTask;
import com.github.liuche51.easyTaskX.cluster.task.master.*;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        CommitSaveTransactionTask task = new CommitSaveTransactionTask();
        task.start();
        return task;
    }

    /**
     * 启动批量事务数据删除任务
     */
    public static TimerTask startCommitDelTransactionTask() {
        CommitDelTransactionTask task = new CommitDelTransactionTask();
        task.start();
        return task;
    }

    /**
     * 启动批量事务数据取消提交任务
     */
    public static TimerTask startCancelSaveTransactionTask() {
        CancelSaveTransactionTask task = new CancelSaveTransactionTask();
        task.start();
        return task;
    }

    /**
     * 启动重试取消保持任务
     */
    public static TimerTask startRetryCancelSaveTransactionTask() {
        RetryCancelSaveTransactionTask task = new RetryCancelSaveTransactionTask();
        task.start();
        return task;
    }

    /**
     * 启动重试删除任务
     */
    public static TimerTask startRetryDelTransactionTask() {
        RetryDelTransactionTask task = new RetryDelTransactionTask();
        task.start();
        return task;
    }

    /**
     * 节点对leader的心跳。
     */
    public static TimerTask startHeartBeat() {
        HeartbeatsTask task = new HeartbeatsTask();
        task.start();
        return task;
    }

    /**
     * 启动点定时从leader获取注册表更新任务
     */
    public static TimerTask startUpdateRegeditTask() {
        FollowRequestUpdateRegeditTask task = new FollowRequestUpdateRegeditTask();
        task.start();
        return task;
    }
    /**
     * 启动点定时从leader更新Client列表。
     */
    public static TimerTask startBrokerUpdateClientsTask() {
        BrokerUpdateClientsTask task = new BrokerUpdateClientsTask();
        task.start();
        return task;
    }

    /**
     * Broker定时从leader获取注册表最新信息
     * 覆盖本地信息
     *
     * @return
     */
    public static boolean requestUpdateRegedit() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowRequestLeaderSendRegedit).setSource(NodeService.getConfig().getAddress())
                    .setBody(StringConstant.BROKER);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                NodeDto.Node node = NodeDto.Node.parseFrom(respPack.getRespbody());
                NodeService.CURRENTNODE.setBakLeader(node.getBakleader());
                dealUpdate(node);
                return true;
            } else {
                log.info("normally exception!requestUpdateRegedit() failed.");
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 处理注册表更新
     * @param node
     */
    public static void dealUpdate(NodeDto.Node node) {
        NodeDto.NodeList slaveNodes = node.getSalves();
        ConcurrentHashMap<String, BaseNode> follows = new ConcurrentHashMap<>();
        slaveNodes.getNodesList().forEach(x -> {
            follows.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeDto.NodeList masterNodes = node.getMasters();
        ConcurrentHashMap<String, BaseNode> leaders = new ConcurrentHashMap<>();
        masterNodes.getNodesList().forEach(x -> {
            leaders.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeService.CURRENTNODE.setSlaves(follows);
        NodeService.CURRENTNODE.setMasters(leaders);
    }
}
