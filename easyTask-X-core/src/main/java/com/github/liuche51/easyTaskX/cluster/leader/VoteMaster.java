package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.enume.DataStatusEnum;
import com.github.liuche51.easyTaskX.enume.NodeStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 选Master
 */
public class VoteMaster {
    private static final Logger log = LoggerFactory.getLogger(VoteMaster.class);

    /**
     * leader从slaves中选择新的master
     *1、优先选取数据同步状态为已同步的节点
     * @param oldmaster
     * @return
     */
    public static RegNode voteNewMaster( RegBroker oldmaster) {
        Collection<RegNode> salves = oldmaster.getSlaves().values();
        RegNode newMaster=null;
        List<RegNode> temp = salves.stream().filter(x -> x.getDataStatus().intValue() == 1).collect(Collectors.toList());
        if(temp!=null&&temp.size()>0){
            newMaster= temp.get(0);
        }else {
            RegNode[] regNodes = salves.toArray(new RegNode[]{});
            newMaster= regNodes[0];
        }
        VoteMaster.updateNodeRegedit(oldmaster,newMaster);
        return newMaster;
    }

    /**
     * 新master选举后，leader更新注册表
     * 1、将当前master节点移出注册表
     * 2、将当前master的slave节点的从其master集合中移出
     *
     * @param oldmaster
     * @param newMaster
     */
    private static void updateNodeRegedit(RegBroker oldmaster,RegNode newMaster) {
        LeaderService.BROKER_REGISTER_CENTER.remove(oldmaster.getAddress());
        Map<String, RegNode> slaves = oldmaster.getSlaves();
        if (slaves.size() > 0) {
            Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
            while (items.hasNext()) {
                RegNode regNode = items.next().getValue();
                RegBroker slave = LeaderService.BROKER_REGISTER_CENTER.get(regNode.getAddress());
                if (slave != null) {
                    slave.getMasters().remove(oldmaster.getAddress());
                    if(slave.getAddress().equals(newMaster.getAddress())){
                        slave.setNodeStatus(NodeStatusEnum.RECOVERING);
                        slave.setDataStatus(DataStatusEnum.UNSYNC);
                    }

                }
            }
        }
    }
}
