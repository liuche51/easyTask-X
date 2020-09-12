package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * leader类
 */
public class SliceLeaderUtil {
    private static final Logger log = LoggerFactory.getLogger(SliceLeaderUtil.class);
    /**
     * 同步任务数据到follow，批量方式
     *用于将数据同步给新follow
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
        boolean ret =NettyMsgService.sendSyncMsgWithCount(builder,client,ClusterService.getConfig().getTryCount(),5);
        return ret;
    }
}
