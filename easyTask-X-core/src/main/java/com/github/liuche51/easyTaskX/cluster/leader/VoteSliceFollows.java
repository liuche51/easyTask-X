package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;

import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyConnectionFactory;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.util.exception.VotedException;
import com.github.liuche51.easyTaskX.util.exception.VotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * leader选举follow。
 * 使用多线程互斥机制
 */
public class VoteSliceFollows {
    private static final Logger log = LoggerFactory.getLogger(VoteSliceFollows.class);
    private static volatile boolean selecting = false;//选举状态。多线程控制
    private static ReentrantLock lock = new ReentrantLock();//选举互斥锁

    public static boolean isSelecting() {
        return selecting;
    }

    /**
     * 节点启动初始化选举follows。
     * 不存在多线程情况，不需要考虑
     *
     * @return
     */
    public static void initSelectFollows(RegisterNode regNode) throws Exception {
        int count = ClusterService.getConfig().getBackupCount();
        List<String> availableFollows = VoteSliceFollows.getAvailableFollows(regNode);
        List<Node> follows = VoteSliceFollows.selectFollows(count, availableFollows);
        if (follows.size() < count) {
            log.info("follows.size() < count,so start to initSelectFollows");
            initSelectFollows(regNode);//数量不够递归重新选VoteFollows.selectFollows中
        } else {
            ConcurrentHashMap<String, Node> follows2 = new ConcurrentHashMap<>(follows.size());
            follows.forEach(x -> {
                follows2.put(x.getAddress(), x);
            });
            regNode.getNode().setFollows(follows2);
            ClusterService.CURRENTNODE.setFollows(follows2);
        }
    }

    /**
     * 选择新follow
     * leader同步数据失败或心跳检测失败，则进入选新follow程序
     *
     * @return
     */
    public static Node selectNewFollow(Node oldFollow) throws Exception {
        if (selecting) throw new VotingException("cluster is voting new follow,please retry later.");
        selecting = true;
        List<Node> follows = null;
        try {
            lock.lock();
            ClusterService.CURRENTNODE.getFollows().remove(oldFollow.getAddress());//移除失效的follow
            NettyConnectionFactory.getInstance().removeHostPool(oldFollow.getAddress());
            log.info("leader remove follow {}", oldFollow.getAddress());

            //多线程下，如果follows已经选好，则让客户端重新提交任务。以后可以优化为获取选举后的follow
            if (ClusterService.CURRENTNODE.getFollows() != null && ClusterService.CURRENTNODE.getFollows().size() >= ClusterService.getConfig().getBackupCount())
                throw new VotedException("cluster is voted follow,please retry again.");
            List<String> availableFollows = getAvailableFollows(Arrays.asList(oldFollow.getAddress()));
            follows = selectFollows(1, availableFollows);
            if (follows.size() < 1)
                selectNewFollow(oldFollow);//数量不够递归重新选
            else {
                Node newFollow = follows.get(0);
                newFollow.setDataStatus(NodeSyncDataStatusEnum.UNSYNC);//选举成功，将新follow数据同步状态标记为未同步
                ClusterService.CURRENTNODE.getFollows().put(newFollow.getAddress(), newFollow);
            }

        } finally {
            selecting = false;//复原选举装填
            lock.unlock();
        }
        if (follows == null || follows.size() == 0)
            throw new Exception("cluster is vote follow failed,please retry later.");
        //通知follows当前Leader位置
        SliceLeaderUtil.notifyFollowsLeaderPosition(follows, ClusterService.getConfig().getTryCount(), 5);
        SliceLeaderUtil.notifyClusterLeaderUpdateRegeditForASync(ClusterService.CURRENTNODE.getFollows(), ClusterService.getConfig().getTryCount(), 5);
        return follows.get(0);
    }

    /**
     * 从zk获取可用的follow，并排除自己
     *
     * @return
     */
    private static List<String> getAvailableFollows(RegisterNode regNode) throws Exception {
        int count = ClusterService.getConfig().getBackupCount();
        List<String> availableFollows = ClusterLeaderService.getRegisteredBokers();
        //排除自己
        Optional<String> temp = availableFollows.stream().filter(x -> {
            try {
                return x.equals(regNode.getNode().getAddress());
            } catch (Exception e) {
                log.error("", e);
                return false;
            }
        }).findFirst();
        if (temp.isPresent())
            availableFollows.remove(temp.get());
        //排除现有的
        Iterator<Map.Entry<String, Node>> items = regNode.getNode().getFollows().entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Node> item = items.next();
            Optional<String> temp1 = availableFollows.stream().filter(y -> y.equals(item.getValue().getAddress())).findFirst();
            if (temp1.isPresent())
                availableFollows.remove(temp1.get());
        }
        if (availableFollows.size() < count - ClusterService.CURRENTNODE.getFollows().size())//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
        {
            log.info("availableFollows is not enough! only has {},current own {}", availableFollows.size(), ClusterService.CURRENTNODE.getFollows().size());
            Thread.sleep(1000);
            return getAvailableFollows(regNode);
        } else
            return availableFollows;
    }

    /**
     * 从可用follows中选择若干个follow
     *
     * @param count            需要的数量
     * @param availableFollows 可用follows
     */
    private static List<Node> selectFollows(int count, List<String> availableFollows) throws InterruptedException {
        List<Node> follows = new LinkedList<>();//备选follows
        int size = availableFollows.size();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(availableFollows.size());//随机生成的随机数范围就变成[0,size)。注意这里size会动态变动。
            String ret = availableFollows.get(index);
            RegisterNode regNode2 = ClusterLeaderService.BROKER_REGISTER_CENTER.get(ret);
            Node newFollow = regNode2.getNode();
            availableFollows.remove(index);
            if (follows.size() < count) {
                follows.add(newFollow);
            } else break;//已选数量够了就跳出
        }
        if (follows.size() < count) Thread.sleep(1000);//此处防止不满足条件时重复高频递归本方法
        return follows;
    }
    public static boolean updateRegedit(Map<String, RegisterNode> brokers,String oldLeader){
        Iterator<Map.Entry<String, Node>> items = brokers.get(oldLeader).getNode().getFollows().entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Node> item = items.next();
            Node node = item.getValue();
            node.getLeaders().remove(oldLeader);
        }
        brokers.remove(oldLeader);
        return true;
    }
}
