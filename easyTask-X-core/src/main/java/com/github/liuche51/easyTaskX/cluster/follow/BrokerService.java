package com.github.liuche51.easyTaskX.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.VoteSlave;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.*;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.master.*;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.*;
import com.github.liuche51.easyTaskX.util.exception.VotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Broker
 */
public class BrokerService {

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
        BrokerRequestUpdateRegeditTask task = new BrokerRequestUpdateRegeditTask();
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
     * 启动Broker通知客户端提交的任务同步结果反馈
     */
    public static TimerTask startBrokerNotifyClientSubmitTaskResultTask() {
        BrokerNotifyClientSubmitTaskResultTask task = new BrokerNotifyClientSubmitTaskResultTask();
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
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENT_NODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                NodeDto.Node node = NodeDto.Node.parseFrom(respPack.getRespbody());
                dealUpdate(node);
                return true;
            } else {
                LogUtil.info("normally exception!requestUpdateRegedit() failed.");
            }
        } catch (Exception e) {
            LogUtil.error("", e);
        }
        return false;
    }

    /**
     * 处理leader推送过来的注册表更新。
     * 1、leader重新选举了master或slave时，leader主动通知broker。
     * 2、broker会定时从leader请求最新的注册表信息同步到本地。防止期间数据不一致性问题，做到最终一致性
     * 3、更新备用leader、最新master集合、最新slave集合
     *
     * @param node
     */
    public static void dealUpdate(NodeDto.Node node) {
        NodeService.CURRENT_NODE.setBakLeader(node.getBakleader());
        NodeDto.NodeList slaveNodes = node.getSalves();
        ConcurrentHashMap<String, BaseNode> slaves = new ConcurrentHashMap<>();
        slaveNodes.getNodesList().forEach(x -> {
            slaves.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeDto.NodeList masterNodes = node.getMasters();
        ConcurrentHashMap<String, BaseNode> masters = new ConcurrentHashMap<>();
        masterNodes.getNodesList().forEach(x -> {
            masters.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
        });
        NodeService.CURRENT_NODE.setSlaves(slaves);
        NodeService.CURRENT_NODE.setMasters(masters);
        BrokerUtil.updateMasterBinlogInfo(masters);
    }

}
