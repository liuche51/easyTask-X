package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.NodeService;

import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
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
 * leader选举slave。
 * 使用多线程互斥机制
 */
public class VoteSlave {
    private static final Logger log = LoggerFactory.getLogger(VoteSlave.class);
    private static volatile boolean selecting = false;//选举状态。多线程控制
    private static ReentrantLock lock = new ReentrantLock();//选举互斥锁

    public static boolean isSelecting() {
        return selecting;
    }

    /**
     * 节点启动初始化选举follows。
     *
     * @return
     */
    public static List<RegNode> initVoteSlaves(RegBroker regNode) throws Exception {
        if (selecting) throw new VotingException(String.format("[%s] is voting a new follow",regNode.getAddress()));
        selecting = true;
        int count = NodeService.getConfig().getBackupCount();
        try {
            lock.lock();
            List<String> availableFollows = VoteSlave.getAvailableFollows(regNode);
            List<RegNode> follows = VoteSlave.voteFollows(count, availableFollows);
            if (follows.size() < count) {
                log.info("[{}] follows.size() < count,so retry to initVoteFollows()",regNode.getAddress());
               return initVoteSlaves(regNode);//数量不够递归重新选VoteFollows.selectFollows中
            } else {
                ConcurrentHashMap<String, RegNode> follows2 = new ConcurrentHashMap<>(follows.size());
                follows.forEach(x -> {
                    follows2.put(x.getAddress(), x);
                });
                updateRegedit(regNode, follows2);
                return follows;
            }
        } finally {
            selecting = false;//复原选举状态
            lock.unlock();
        }
    }

    /**
     * 选择新follow。旧follow失效
     *
     * @return
     */
    public static RegNode voteNewSlave(RegBroker regNode, RegNode oldFollow) throws Exception {
        if (selecting) throw new VotingException(String.format("[%s] is voting a new follow",regNode.getAddress()));
        selecting = true;
        List<RegNode> follows = null;
        try {
            lock.lock();
            //多线程下，如果follows已经选好，则让客户端重新提交任务。以后可以优化为获取选举后的follow
            if (regNode.getSlaves().size() >= NodeService.getConfig().getBackupCount())
                throw new VotedException(String.format("[%s] has voted a new follow.",regNode.getAddress()));
            List<String> availableFollows = getAvailableFollows(regNode);
            follows = voteFollows(1, availableFollows);
            if (follows.size() < 1)
                voteNewSlave(regNode, oldFollow);//数量不够递归重新选
            else {
                RegNode newFollow = follows.get(0);
                updateRegedit(regNode, oldFollow.getAddress(), newFollow);
            }

        } finally {
            selecting = false;//复原选举装填
            lock.unlock();
        }
        if (follows == null || follows.size() == 0)
            throw new Exception(String.format("[%s] is vote follow failed",regNode.getAddress()));
        return follows.get(0);
    }

    /**
     * 从zk获取可用的follow，并排除自己
     *
     * @return
     */
    private static List<String> getAvailableFollows(RegBroker regNode) throws Exception {
        int count = NodeService.getConfig().getBackupCount();
        Iterator<Map.Entry<String, RegBroker>> items = LeaderService.BROKER_REGISTER_CENTER.entrySet().iterator();
        List<String> availableFollows = new ArrayList<>(LeaderService.BROKER_REGISTER_CENTER.size());
        while (items.hasNext()) {
            availableFollows.add(items.next().getKey());
        }
        //排除自己
        Optional<String> temp = availableFollows.stream().filter(x -> {
            try {
                return x.equals(regNode.getAddress());
            } catch (Exception e) {
                log.error("", e);
                return false;
            }
        }).findFirst();
        if (temp.isPresent())
            availableFollows.remove(temp.get());
        //排除现有的
        Iterator<Map.Entry<String, RegNode>> items2 = regNode.getSlaves().entrySet().iterator();
        while (items2.hasNext()) {
            Map.Entry<String, RegNode> item = items2.next();
            Optional<String> temp1 = availableFollows.stream().filter(y -> y.equals(item.getValue().getAddress())).findFirst();
            if (temp1.isPresent())
                availableFollows.remove(temp1.get());
        }
        if (availableFollows.size() < count - NodeService.CURRENTNODE.getSlaves().size())//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
        {
            log.info("[{}] availableFollows is not enough! only has {},current own {}",regNode.getAddress(), availableFollows.size(), regNode.getSlaves().size());
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
    private static List<RegNode> voteFollows(int count, List<String> availableFollows) throws InterruptedException {
        List<RegNode> follows = new LinkedList<>();//备选follows
        int size = availableFollows.size();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(availableFollows.size());//随机生成的随机数范围就变成[0,size)。注意这里size会动态变动。
            String ret = availableFollows.get(index);
            RegBroker regNode2 = LeaderService.BROKER_REGISTER_CENTER.get(ret);
            RegNode newFollow =new RegNode(regNode2);
            availableFollows.remove(index);
            if (follows.size() < count) {
                follows.add(newFollow);//这里一定要用新对象。否则对象重用会导致属性值也被公用了
            } else break;//已选数量够了就跳出
        }
        if (follows.size() < count) Thread.sleep(1000);//此处防止不满足条件时重复高频递归本方法
        return follows;
    }

    /**
     * leader通知master，已经选出新follow。
     *
     * @param leader
     * @param newFollowAddress
     */
    public static void notifySliceLeaderVoteNewFollow(RegBroker leader, String newFollowAddress, String oldFollowAddress) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LeaderNotifyMasterVoteNewSlave)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(newFollowAddress + StringConstant.CHAR_SPRIT_STRING + oldFollowAddress);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, leader.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if(!ret)
                        log.info("normally exception!notifySliceLeaderVoteNewFollow() failed.");
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

    /**
     * 节点初始化选新follows，更新注册表
     *
     * @param regNode
     * @param newfollows
     */
    private static void updateRegedit(RegBroker regNode, ConcurrentHashMap<String, RegNode> newfollows) {
        regNode.setSlaves(newfollows);
        Iterator<Map.Entry<String, RegNode>> items = newfollows.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegNode> item = items.next();
            RegNode node = item.getValue();
            RegBroker followRegnode= LeaderService.BROKER_REGISTER_CENTER.get(node.getAddress());
            followRegnode.getMasters().put(regNode.getAddress(), new RegNode(regNode));
        }
    }

    /**
     * 旧follow失效，选新follow。更新注册表
     *
     * @param regNode
     * @param oldFollow
     */
    private static void updateRegedit(RegBroker regNode, String oldFollow, RegNode newFollow) {
        regNode.getSlaves().remove(oldFollow);
        newFollow.setDataStatus(NodeSyncDataStatusEnum.UNSYNC);//选举成功，将新follow数据同步状态标记为未同步
        regNode.getSlaves().put(newFollow.getAddress(), newFollow);
        RegBroker newFollowRegNode= LeaderService.BROKER_REGISTER_CENTER.get(newFollow.getAddress());
        newFollowRegNode.getMasters().put(regNode.getAddress(),new RegNode(regNode));
   }
}
