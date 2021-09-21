package com.github.liuche51.easyTaskX.cluster.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.*;

import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.*;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SaveTaskTCC {
    /**
     * 提交任务事务第一阶段。
     * 先记入事务表，等到第二阶段提交确认
     *
     * @param schedule
     * @param slave
     * @throws Exception
     */
    public static void trySave(String transactionId, Schedule schedule, BaseNode slave) throws Exception {
        schedule.setTransactionId(transactionId);
        schedule.setSource(Util.getSource(schedule.getSource()));
        TranlogSchedule transactionLog = new TranlogSchedule();
        transactionLog.setId(transactionId);
        transactionLog.setContent(JSONObject.toJSONString(schedule));
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setSlaves(slave.getAddress());
        TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        ScheduleDto.Schedule s = schedule.toScheduleDto();
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranTrySaveTask).setSource(NodeService.getConfig().getAddress())
                .setBodyBytes(s.toByteString());
        NettyClient client = slave.getClientWithCount(1);
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
        if (!ret) {
            throw new Exception("ret=false");
        }
    }

    /**
     * 确认提交任务。第二阶段
     * 1、第一步，通知slave可以提交新增事务数据了
     * 2、第二部，将本地新增任务事务标记为以确认
     * 3、如果第二步失败了，上层调用就会进入事务回滚
     *
     * @param transactionId
     * @param slave
     * @throws Exception
     */
    public static void confirm(String transactionId, BaseNode slave) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranConfirmSaveTask).setSource(NodeService.getConfig().getAddress())
                .setBody(transactionId);
        NettyClient client = slave.getClientWithCount(1);
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
        if (!ret) {
            throw new Exception("ret=false");
        }
        TranlogScheduleDao.updateStatusById(transactionId, TransactionStatusEnum.CONFIRM);
    }

    /**
     * 事务回滚阶段。
     *
     * @param transactionId
     * @throws Exception
     */
    public static void cancel(String transactionId) throws Exception {
        TranlogScheduleDao.updateStatusById(transactionId, TransactionStatusEnum.CANCEL);//自己优先标记需回滚
        //网binglog写一条删除日志
    }
}
