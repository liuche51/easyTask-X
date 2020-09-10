package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.dto.RegisterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterLeaderService {
    private static final Logger log = LoggerFactory.getLogger(VoteSliceFollows.class);
    /**
     * 集群NODE注册表
     */
    public static ConcurrentHashMap<String, RegisterNode> BROKER_REGISTER_CENTER = new ConcurrentHashMap<>(10);
    /**
     * 集群CLIENT注册表
     */
    public static ConcurrentHashMap<String, RegisterNode> CLIENT_REGISTER_CENTER = new ConcurrentHashMap<>(10);

    public static List<String> getRegisteredBokers() {
        Iterator<Map.Entry<String, RegisterNode>> items = BROKER_REGISTER_CENTER.entrySet().iterator();
        List<String> list = new ArrayList<>(BROKER_REGISTER_CENTER.size());
        while (items.hasNext()) {
            list.add(items.next().getKey());
        }
        return list;
    }

    /**
     * 通知节点更新注册表信息
     *
     * @param nodes
     */
    public static void notifyNodeUpdateRegedit(List<Node> nodes) {

        nodes.forEach(x -> {
            ClusterService.getConfig().getClusterPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.NOTIFY_NODE_UPDATE_REGEDIT)
                                .setSource(ClusterService.CURRENTNODE.getAddress());
                        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, x.getClient(), ClusterService.getConfig().getTryCount(), 5);
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            });
        });
    }

    /**
     * 请求获取当前节点最新注册表信息。
     * 覆盖本地信息
     *
     * @param tryCount
     * @param waiteSecond
     * @return
     */
    public static boolean requestUpdateRegedit(int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.UPDATE_REGEDIT).setSource(ClusterService.getConfig().getAddress());
            Dto.Frame frame = NettyMsgService.sendSyncMsg(ClusterService.CURRENTNODE.getClusterLeader().getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                NodeDto.Node node = NodeDto.Node.parseFrom(result.getBodyBytes());
                NodeDto.NodeList clientNodes = node.getClients();
                ConcurrentHashMap<String, Node> clients = new ConcurrentHashMap<>();
                clientNodes.getNodesList().forEach(x -> {
                    clients.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
                });
                NodeDto.NodeList followNodes = node.getClients();
                ConcurrentHashMap<String, Node> follows = new ConcurrentHashMap<>();
                followNodes.getNodesList().forEach(x -> {
                    follows.put(x.getHost() + ":" + x.getPort(), new Node(x.getHost(), x.getPort()));
                });
                ClusterService.CURRENTNODE.setFollows(follows);
                ClusterService.CURRENTNODE.setClients(clients);
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("updateRegedit.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        log.info("updateRegedit()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + ClusterService.CURRENTNODE.getClusterLeader().getAddress());
        try {
            Thread.sleep(waiteSecond * 1000);
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return requestUpdateRegedit(tryCount, waiteSecond);
    }
}
