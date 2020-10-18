package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class VoteMaster {
    private static final Logger log = LoggerFactory.getLogger(VoteMaster.class);

    /**
     * leader从follows中选择新的master
     *
     * @param follows
     * @return
     */
    public static RegNode voteNewLeader(Map<String, RegNode> follows) {
        Iterator<Map.Entry<String, RegNode>> items = follows.entrySet().iterator();
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
     * leader通知follows。旧leader失效，leader已选新leader。
     *
     * @param follows
     * @param newLeader
     * @param oldLeader
     * @return
     */
    public static boolean notifySliceFollowsNewLeader(Map<String, RegNode> follows, String newLeader, String oldLeader) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum. NotifySlaveNewLeader).setSource(NodeService.getConfig().getAddress())
                    .setBody(oldLeader + StringConstant.CHAR_SPRIT_STRING + newLeader);
            Iterator<Map.Entry<String, RegNode>> items = follows.entrySet().iterator();
            while (items.hasNext()) {
                Map.Entry<String, RegNode> item = items.next();
                RegNode node = item.getValue();
                boolean ret=NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                if(!ret)
                    log.info("normally exception!notifySliceFollowsNewLeader() failed.");
            }
            return true;
        } catch (Exception e) {
            log.error("", e);
        }
        return false;
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
