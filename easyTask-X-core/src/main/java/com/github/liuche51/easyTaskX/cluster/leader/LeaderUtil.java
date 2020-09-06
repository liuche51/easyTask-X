package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.cluster.ClusterUtil;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * leader类
 */
public class LeaderUtil {
    private static final Logger log = LoggerFactory.getLogger(LeaderUtil.class);

    /**
     * 通知follows当前Leader位置。异步调用即可
     *
     * @return
     */
    public static boolean notifyFollowsLeaderPosition(List<Node> follows, int tryCount, int waiteSecond) {
        ClusterService.getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (follows != null) {
                    follows.forEach(x -> {
                        notifyFollowLeaderPosition(x, tryCount,waiteSecond);
                    });
                }
            }
        });
        return true;
    }
    /**
     * 通知集群leader更新注册表新follow信息。异步调用即可
     *
     * @return
     */
    public static boolean notifyClusterLeaderUpdateRegeditForASync(Map<String,Node> follows, int tryCount, int waiteSecond) {
        ClusterService.getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
               notifyClusterLeaderUpdateRegedit(follows,tryCount,waiteSecond);
            }
        });
        return true;
    }
    /**
     * @param follows
     * @param tryCount
     * @return
     */
    public static boolean notifyClusterLeaderUpdateRegedit(Map<String,Node> follows, int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.UPDATE_CLUSTER_LEADER_BROKER_REGEDIT).setSource(ClusterService.getConfig().getAddress())
                    .setBody("follow|"+ JSONObject.toJSONString(follows));
            Dto.Frame frame = NettyMsgService.sendSyncMsg(ClusterService.CURRENTNODE.getClusterLeader().getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("notifyClusterLeaderUpdate.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        log.info("notifyClusterLeaderUpdate()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + ClusterService.CURRENTNODE.getClusterLeader().getAddress());
        try {
            Thread.sleep(waiteSecond*1000);
        } catch (InterruptedException e) {
            log.error("",e);
        }
        return notifyClusterLeaderUpdateRegedit(follows, tryCount,waiteSecond);
    }
    /**
     * @param follow
     * @param tryCount
     * @return
     */
    public static boolean notifyFollowLeaderPosition(Node follow, int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.SYNC_LEADER_POSITION).setSource(ClusterService.getConfig().getAddress())
                    .setBody(ClusterService.getConfig().getAddress());
            Dto.Frame frame = NettyMsgService.sendSyncMsg(follow.getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("notifyFollowLeaderPosition.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        log.info("notifyFollowLeaderPosition()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + follow.getAddress());
        try {
            Thread.sleep(waiteSecond*1000);
        } catch (InterruptedException e) {
            log.error("",e);
        }
        return notifyFollowLeaderPosition(follow, tryCount,waiteSecond);
    }
    /**
     * 同步任务数据到follow，批量方式
     *用于将数据同步给新follow
     *暂时不支持失败进入选新follow流程。代码注释掉
     * 目前仅在leader心跳follow是否存活那边进行选新follow流程
     * @param schedules
     * @param follow
     * @return
     * @throws InterruptedException
     */
    public static boolean syncDataToFollowBatch(List<Schedule> schedules, Node follow) throws Exception {
        ScheduleDto.ScheduleList.Builder builder0=ScheduleDto.ScheduleList.newBuilder();
        for(Schedule schedule:schedules){
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            builder0.addSchedules(s);
        }
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LEADER_SYNC_DATA_TO_NEW_FOLLOW).setSource(ClusterService.getConfig().getAddress())
                .setBodyBytes(builder0.build().toByteString());
        NettyClient client = follow.getClientWithCount(ClusterService.getConfig().getTryCount());
       /* if (client == null) {
            log.info("client == null,so start to syncDataToFollowBatch.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollowBatch(schedules, newFollow);
        }*/
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), ClusterService.getConfig().getTryCount());
      /*  if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to syncDataToFollowBatch.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollowBatch(schedules, newFollow);
        }*/
        return ret;
    }
}
