package com.github.liuche51.easyTaskX.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VoteSliceLeader {
    private static final Logger log = LoggerFactory.getLogger(VoteSliceLeader.class);

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

    public static boolean notifySliceFollowsNewLeader(Map<String, Node> follows, String newLeader, String oldLeader, int tryCount, int waiteSecond) {
        Iterator<Map.Entry<String, Node>> items = follows.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Node> item = items.next();
            Node node = item.getValue();
            notifySliceFollowNewLeader(node, oldLeader, newLeader, tryCount, waiteSecond);
        }
        return true;
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
    private static boolean notifySliceFollowNewLeader(Node follow, String oldLeader, String newLeader, int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.NOTIFY_SLICE_FOLLOW_NEW_LEADER).setSource(ClusterService.getConfig().getAddress())
                    .setBody(oldLeader + "|" + newLeader);
            Dto.Frame frame = NettyMsgService.sendSyncMsg(follow.getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("notifySliceFollowsNewLeader.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        log.info("notifySliceFollowsNewLeader()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + follow.getAddress());
        try {
            Thread.sleep(waiteSecond * 1000);
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return notifySliceFollowNewLeader(follow, oldLeader, newLeader, tryCount, waiteSecond);
    }
}
