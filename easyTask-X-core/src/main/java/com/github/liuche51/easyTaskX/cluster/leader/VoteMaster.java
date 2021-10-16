package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
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
     * @param slaves
     * @return
     */
    public static RegNode voteNewMaster(Map<String, RegNode> slaves) {
        Collection<RegNode> salves = slaves.values();
        List<RegNode> temp = salves.stream().filter(x -> x.getDataStatus().intValue() == 1).collect(Collectors.toList());
        if(temp!=null&&temp.size()>0){
            return temp.get(0);
        }else {
            RegNode[] regNodes = salves.toArray(new RegNode[]{});
            return regNodes[0];
        }
    }

    /**
     * 新master选举后，leader更新注册表
     *
     * @param regBroker
     */
    public static void updateRegedit(RegBroker regBroker) {
        LeaderService.BROKER_REGISTER_CENTER.remove(regBroker.getAddress());
        Map<String, RegNode> slaves = regBroker.getSlaves();
        if (slaves.size() > 0) {
            Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
            while (items.hasNext()) {
                RegNode regNode = items.next().getValue();
                RegBroker slave = LeaderService.BROKER_REGISTER_CENTER.get(regNode.getAddress());
                if (slave != null) {
                    slave.getMasters().remove(regBroker.getAddress());
                }
            }
        }
    }
}
