package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class VoteSliceLeader {
    private static final Logger log = LoggerFactory.getLogger(VoteSliceLeader.class);

    /**
     * 集群leader从follows中选择新的分片leader
     *
     * @param follows
     * @return
     */
    public static Node voteNewLeader(Map<String, Node> follows) {
        Iterator<Map.Entry<String, Node>> items = follows.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Node> item = items.next();
            Node node = item.getValue();
            if (NodeSyncDataStatusEnum.SYNC == node.getDataStatus()) {
                return node;
            }
        }
        return null;
    }

    /**
     * 集群leader通知follows。旧leader失效，集群leader已选新leader。
     *
     * @param follows
     * @param newLeader
     * @param oldLeader
     * @return
     */
    public static boolean notifySliceFollowsNewLeader(Map<String, Node> follows, String newLeader, String oldLeader) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum. NOTIFY_SLICE_FOLLOW_NEW_LEADER).setSource(ClusterService.getConfig().getAddress())
                    .setBody(oldLeader + "|" + newLeader);
            Iterator<Map.Entry<String, Node>> items = follows.entrySet().iterator();
            while (items.hasNext()) {
                Map.Entry<String, Node> item = items.next();
                Node node = item.getValue();
                boolean ret=NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), ClusterService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
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
     * 新分片leader选举后，集群leader更新注册表
     * 这种情况只需要将失效的旧leader从注册表中移除即可。至于这个节点可能是其他节点leader的follow
     * 则不需要本次处理。待心跳存活检查任务分析那个leader的follow时处理掉即可
     *
     * @param oldLeader
     * @return
     */
    public static void updateRegedit(String oldLeader) {
        ClusterLeaderService.BROKER_REGISTER_CENTER.remove(oldLeader);
    }
}
