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

import java.util.Iterator;
import java.util.Map;

/**
 * 选Master
 */
public class VoteMaster {
    private static final Logger log = LoggerFactory.getLogger(VoteMaster.class);

    /**
     * leader从slaves中选择新的master
     *
     * @param slaves
     * @return
     */
    public static RegNode voteNewMaster(Map<String, RegNode> slaves) {
        Iterator<Map.Entry<String, RegNode>> items = slaves.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, RegNode> item = items.next();
            RegNode node = item.getValue();
            if (NodeSyncDataStatusEnum.SYNC == node.getDataStatus()) {
                return node;
            }
        }
        return null;
    }

    /**
     * 新master选举后，leader更新注册表
     * @param regBroker
     */
    public static void updateRegedit(RegBroker regBroker){
        LeaderService.BROKER_REGISTER_CENTER.remove(regBroker.getAddress());
        Map<String, RegNode> follows=regBroker.getSlaves();
        if(follows.size()>0){
            Iterator<Map.Entry<String, RegNode>> items=follows.entrySet().iterator();
            while (items.hasNext()){
                RegNode regNode=items.next().getValue();
                RegBroker follow= LeaderService.BROKER_REGISTER_CENTER.get(regNode.getAddress());
                if(follow!=null){
                    follow.getMasters().remove(regBroker.getAddress());
                }
            }
        }
    }
}
