package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.enume.DataStatusEnum;
import com.github.liuche51.easyTaskX.enume.NodeStatusEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.exception.VotedException;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * leader选举slave。
 * 使用多线程互斥机制
 */
public class VoteSlave {
    private static volatile boolean selecting = false;//选举状态。多线程控制
    private static ReentrantLock lock = new ReentrantLock();//选举互斥锁

    public static boolean isSelecting() {
        return selecting;
    }

    /**
     * 节点启动初始化选举slave。
     *
     * @return
     */
    public static List<RegNode> initVoteSlaves(RegBroker regNode) throws Exception {
        if (selecting) throw new VotingException(String.format("[%s] is voting a new slave", regNode.getAddress()));
        selecting = true;
        int count = BrokerService.getConfig().getBackupCount();
        try {
            lock.lock();
            List<String> availableSlaves = VoteSlave.getAvailableSlave(regNode);
            List<RegNode> slaves = VoteSlave.voteSlaves(count, availableSlaves);
            if (slaves.size() < count) {
                LogUtil.info("[{}] slaves.size() < count,so retry to initVoteSlaves()", regNode.getAddress());
                return initVoteSlaves(regNode);//数量不够递归重新选
            } else {
                ConcurrentHashMap<String, RegNode> slaves2 = new ConcurrentHashMap<>(slaves.size());
                slaves.forEach(x -> {
                    slaves2.put(x.getAddress(), x);
                });
                updateNodeRegedit(regNode, slaves2);
                return slaves;
            }
        } finally {
            selecting = false;//复原选举状态
            lock.unlock();
        }
    }

    /**
     * 选择新slave。旧slave失效。
     * 1、同时更新当前master和新slave的注册表信息
     *
     * @return
     */
    public static RegNode voteNewSlave(RegBroker regNode, RegNode oldSlave) throws Exception {
        if (selecting) throw new VotingException(String.format("[%s] is voting a new slave", regNode.getAddress()));
        selecting = true;
        List<RegNode> slaves = null;
        try {
            lock.lock();
            //多线程下，如果follows已经选好，则让客户端重新提交任务。以后可以优化为获取选举后的follow
            if (regNode.getSlaves().size() >= BrokerService.getConfig().getBackupCount())
                throw new VotedException(String.format("[%s] has voted a new slave.", regNode.getAddress()));
            List<String> availableFollows = getAvailableSlave(regNode);
            slaves = voteSlaves(1, availableFollows);
            if (slaves.size() < 1)
                voteNewSlave(regNode, oldSlave);//数量不够递归重新选
            else {
                RegNode newSlave = slaves.get(0);
                updateNodeRegedit(regNode, oldSlave.getAddress(), newSlave);
            }

        } finally {
            selecting = false;//复原选举装填
            lock.unlock();
        }
        if (slaves == null || slaves.size() == 0)
            throw new Exception(String.format("[%s] is vote slaves failed", regNode.getAddress()));
        return slaves.get(0);
    }

    /**
     * 从zk获取可用的follow，并排除自己
     *
     * @return
     */
    private static List<String> getAvailableSlave(RegBroker regNode) throws Exception {
        int count = BrokerService.getConfig().getBackupCount();
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
                LogUtil.error("", e);
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
        if (availableFollows.size() < count - MasterService.SLAVES.size())//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
        {
            LogUtil.info("[{}] getAvailableSlave is not enough! only has {},current own {}", regNode.getAddress(), availableFollows.size(), regNode.getSlaves().size());
            TimeUnit.SECONDS.sleep(1L);
            return getAvailableSlave(regNode);
        } else
            return availableFollows;
    }

    /**
     * 从可用Brokers中选择若干个slaves
     *
     * @param count            需要的数量
     * @param availableBrokers 可用Brokers
     */
    private static List<RegNode> voteSlaves(int count, List<String> availableBrokers) throws InterruptedException {
        List<RegNode> brokers = new LinkedList<>();//备选brokers
        int size = availableBrokers.size();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(availableBrokers.size());//随机生成的随机数范围就变成[0,size)。注意这里size会动态变动。
            String ret = availableBrokers.get(index);
            RegBroker regNode2 = LeaderService.BROKER_REGISTER_CENTER.get(ret);
            RegNode newslave = new RegNode(regNode2);
            availableBrokers.remove(index);
            if (brokers.size() < count) {
                brokers.add(newslave);//这里一定要用新对象。否则对象重用会导致属性值也被公用了
            } else break;//已选数量够了就跳出
        }
        if (brokers.size() < count)
            TimeUnit.SECONDS.sleep(1L); //此处防止不满足条件时重复高频递归本方法
        return brokers;
    }


    /**
     * 节点初始化选新slaves，更新注册表
     *
     * @param master
     * @param newSlaves
     */
    private static void updateNodeRegedit(RegBroker master, ConcurrentHashMap<String, RegNode> newSlaves) {
        master.setSlaves(newSlaves);
        Iterator<Map.Entry<String, RegNode>> items = newSlaves.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegNode> item = items.next();
            RegNode node = item.getValue();
            RegBroker slaveRegnode = LeaderService.BROKER_REGISTER_CENTER.get(node.getAddress());
            slaveRegnode.getMasters().put(master.getAddress(), new RegNode(master));
        }
    }

    /**
     * 旧Slave失效，选新Slave。更新注册表
     *
     * @param regNode
     * @param oldSlave
     */
    private static void updateNodeRegedit(RegBroker regNode, String oldSlave, RegNode newSlave) {
        regNode.setDataStatus(DataStatusEnum.UNSYNC);
        newSlave.setNodeStatus(NodeStatusEnum.RECOVERING);
        regNode.getSlaves().remove(oldSlave);
        regNode.getSlaves().put(newSlave.getAddress(), newSlave);
        RegBroker newSlaveRegNode = LeaderService.BROKER_REGISTER_CENTER.get(newSlave.getAddress());
        newSlaveRegNode.getMasters().put(regNode.getAddress(), new RegNode(regNode));
    }
}
