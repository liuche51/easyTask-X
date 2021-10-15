package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.BinlogClusterMetaDao;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.LogErrorDao;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.cluster.task.leader.CheckFollowsAliveTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.LogError;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.LogErrorTypeEnum;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LeaderService {
    private static final Logger log = LoggerFactory.getLogger(LeaderService.class);
    /**
     * 集群BROKER注册表LeaderNotifyClientUpdateBrokerChange
     */
    public static ConcurrentHashMap<String, RegBroker> BROKER_REGISTER_CENTER = new ConcurrentHashMap<>(10);
    /**
     * 集群CLIENT注册表
     */
    public static ConcurrentHashMap<String, RegClient> CLIENT_REGISTER_CENTER = new ConcurrentHashMap<>(10);

    /**
     * 集群节点心跳信息收集发送队列
     * 1、leader使用。每个bakleader单独分开
     * 2、bakleader异步消费
     * 3、只有leader一个线程操作，不需支持并发
     */
    public static Map<String, LinkedBlockingQueue<String>> followsHeartbeats = new HashMap<>(2);

    /**
     * 通知follows节点更新注册表信息
     *
     * @param nodes
     * @param type  节点类型  broker和client
     */
    public static void notifyFollowsUpdateRegedit(List<RegNode> nodes, String type) {

        nodes.forEach(x -> {
            LeaderUtil.notifyFollowUpdateRegedit(x.getAddress(), type);
        });
    }

    /**
     * 通知follows更新注册表信息
     *
     * @param nodes
     * @param type  节点类型  broker和client
     */
    public static void notifyFollowsUpdateRegedit(Map<String, RegNode> nodes, String type) {
        Iterator<Map.Entry<String, RegNode>> items = nodes.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyFollowUpdateRegedit(items.next().getValue().getAddress(), type);
        }
    }

    /**
     * 通知Follow更新备用leader信息
     */
    public static void notifyFollowsBakLeaderChanged() {
        String bakLeader = JSONObject.toJSONString(NodeService.CURRENTNODE.getSlaves());
        Iterator<String> items = LeaderService.BROKER_REGISTER_CENTER.keySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyFollowBakLeaderChanged(items.next(), bakLeader);
        }
        Iterator<String> items2 = LeaderService.CLIENT_REGISTER_CENTER.keySet().iterator();
        while (items2.hasNext()) {
            LeaderUtil.notifyFollowBakLeaderChanged(items2.next(), bakLeader);
        }
    }

    /**
     * 通知Broker注册成功。
     *
     * @param broker
     */
    public static void notifyBrokerRegisterSucceeded(BaseNode broker) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyBrokerRegisterSucceeded)
                            .setSource(NodeService.CURRENTNODE.getAddress());
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, broker.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret) {
                        NettyMsgService.writeRpcErrorMsgToDb("Leader通知Broker注册成功。失败！", "com.github.liuche51.easyTaskX.cluster.leader.LeaderService.notifyBrokerRegisterSucceeded");
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * 通知Clinets。Broker发生变更。
     *
     * @param broker
     * @param newMaster
     * @param type      add、delete
     */
    public static void notifyClinetsChangedBroker(String broker, String newMaster, String type) {
        Iterator<Map.Entry<String, RegClient>> items = LeaderService.CLIENT_REGISTER_CENTER.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyClinetChangedBroker(items.next().getValue(), broker, newMaster, type);
        }
    }

    /**
     * 通知Brokers更新Clinet列表变更信息
     *
     * @param client
     * @param type   add、delete
     */
    public static void notifyBrokersChangedClinet(String client, String type) {
        Iterator<Map.Entry<String, RegBroker>> items = LeaderService.BROKER_REGISTER_CENTER.entrySet().iterator();
        while (items.hasNext()) {
            LeaderUtil.notifyBrokerChangedClient(items.next().getValue(), client, type);
        }
    }

    /**
     * leader通知slaves。旧Master失效，leader已选新Master。
     *
     * @param slaves
     * @param newMaster
     * @param oldMaster
     * @return
     */
    public static boolean notifySlaveVotedNewMaster(Map<String, RegNode> slaves, String newMaster, String oldMaster) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotiySlaveVotedNewMaster).setSource(NodeService.getConfig().getAddress())
                    .setBody(oldMaster + StringConstant.CHAR_SPRIT_STRING + newMaster);
            Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
            while (items.hasNext()) {
                Map.Entry<String, RegNode> item = items.next();
                RegNode node = item.getValue();
                boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                if (!ret) {
                    NettyMsgService.writeRpcErrorMsgToDb("leader通知slaves。旧Master失效，leader已选新Master。失败！", "com.github.liuche51.easyTaskX.cluster.leader.LeaderService.notifySlaveVotedNewMaster");
                }
            }
            return true;
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
    }

    /**
     * 启动leader检查所有follows是否存活任务
     */
    public static TimerTask startCheckFollowAliveTask() {
        CheckFollowsAliveTask task = new CheckFollowsAliveTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }

    /**
     * bakleader的心跳同步队列发生变更
     */
    public static void changeFollowsHeartbeats() {
        //找到新加入的slave队列
        Iterator<String> items = NodeService.CURRENTNODE.getSlaves().keySet().iterator();
        while (items.hasNext()) {
            String key = items.next();
            if (!followsHeartbeats.containsKey(key)) {
                followsHeartbeats.put(key, new LinkedBlockingQueue<String>(NodeService.getConfig().getAdvanceConfig().getFollowsHeartbeatsQueueCapacity()));
            }
        }
        // 找到需要移除的失效队列
        Iterator<String> items2 = followsHeartbeats.keySet().iterator();
        while (items2.hasNext()) {
            String key = items2.next();
            if (!NodeService.CURRENTNODE.getSlaves().containsKey(key)) {
                followsHeartbeats.remove(key);
            }
        }
    }

    /**
     * 往所有bakleader队列里心中节点心跳时间信息
     *
     * @param address 当前心跳的节点标识
     * @param regnode 节点类型 RegNodeTypeEnum
     */
    public static void addFollowsHeartbeats(String address, String regnode) {
        followsHeartbeats.values().forEach(x -> {
            boolean ret = x.offer(regnode + StringConstant.CHAR_SPRIT_COMMA + address + StringConstant.CHAR_SPRIT_COMMA + DateUtils.getCurrentDateTime());
            if (!ret) {
                List<LogError> logErrors = new ArrayList<>(1);
                logErrors.add(new LogError("leader往bakleader心跳队列存数据失败，队列已满!", "com.github.liuche51.easyTaskX.cluster.leader.LeaderService.addFollowsHeartbeats", LogErrorTypeEnum.NORMAL));
                LogErrorDao.saveBatch(logErrors);
            }
        });
    }

    /**
     * 查询指定集群元数据数据的binlog数据
     *
     * @param index
     * @return
     * @throws SQLException
     */
    public static List<BinlogClusterMeta> getBinlogClusterMetaByIndex(long index) throws SQLException {
        return BinlogClusterMetaDao.getBinlogClusterMetaByIndex(index, NodeService.getConfig().getAdvanceConfig().getBinlogCount());
    }
}
