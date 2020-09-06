package com.github.liuche51.easyTaskX.cluster;

import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterUtil {
    private static final Logger log = LoggerFactory.getLogger(ClusterUtil.class);

    /**
     * 带重试次数的同步消息发送
     *
     * @param client
     * @param msg
     * @param tryCount
     * @return
     */
    public static boolean sendSyncMsgWithCount(NettyClient client, Object msg, int tryCount) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        Object ret=null;
        Dto.Frame frame=null;
        try {
            ret =  NettyMsgService.sendSyncMsg(client,msg);
            frame = (Dto.Frame) ret;
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult()))
                return true;
            else
                error = result.getMsg();
        }
        catch (Exception e) {
            log.error("sendSyncMsgWithCount exception!error=" + error, e);
        }
        finally {
            tryCount--;
        }
        log.info("sendSyncMsgWithCount error" + error + ",tryCount=" + tryCount + ",objectHost=" + client.getObjectAddress());
        return sendSyncMsgWithCount(client, msg, tryCount);

    }
    public static boolean updateRegedit(int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.UPDATE_REGEDIT).setSource(ClusterService.getConfig().getAddress());
            Dto.Frame frame = NettyMsgService.sendSyncMsg(ClusterService.CURRENTNODE.getClusterLeader().getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                NodeDto.Node node=NodeDto.Node.parseFrom(result.getBodyBytes());
                NodeDto.NodeList clientNodes=node.getClients();
                ConcurrentHashMap<String,Node> clients=new ConcurrentHashMap<>();
                clientNodes.getNodesList().forEach(x->{
                    clients.put(x.getHost()+":"+x.getPort(),new Node(x.getHost(),x.getPort()));
                });
                NodeDto.NodeList followNodes=node.getClients();
                ConcurrentHashMap<String,Node> follows=new ConcurrentHashMap<>();
                followNodes.getNodesList().forEach(x->{
                    follows.put(x.getHost()+":"+x.getPort(),new Node(x.getHost(),x.getPort()));
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
        return updateRegedit(tryCount, waiteSecond);
    }
}
