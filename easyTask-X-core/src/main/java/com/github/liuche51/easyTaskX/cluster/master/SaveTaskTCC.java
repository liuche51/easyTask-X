package com.github.liuche51.easyTaskX.cluster.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.*;

import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
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
     * @param schedule
     * @param follows
     * @throws Exception
     */
    public static void trySave(String transactionId,Schedule schedule, List<BaseNode> follows) throws Exception {
        List<String> cancelHost=follows.stream().map(BaseNode::getAddress).collect(Collectors.toList());
        schedule.setTransactionId(transactionId);
        schedule.setSource(Util.getSource(schedule.getSource()));
        TranlogSchedule transactionLog = new TranlogSchedule();
        transactionLog.setId(transactionId);
        transactionLog.setContent(JSONObject.toJSONString(schedule));
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.SAVE);
        transactionLog.setSlaves(JSONObject.toJSONString(cancelHost));
        TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        Iterator<BaseNode> items = follows.iterator();
        while (items.hasNext()) {
            BaseNode follow = items.next();
            ScheduleSync scheduleSync = new ScheduleSync();
            scheduleSync.setTransactionId(transactionLog.getId());
            scheduleSync.setScheduleId(schedule.getId());
            scheduleSync.setSlave(follow.getAddress());
            scheduleSync.setStatus(ScheduleSyncStatusEnum.SYNCING);
            ScheduleSyncDao.save(scheduleSync);//记录同步状态表
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranTrySaveTask).setSource(NodeService.getConfig().getAddress())
                    .setBodyBytes(s.toByteString());
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if(!ret){
                throw new Exception("ret=false");
            }
        }

    }

    /**
     * 确认提交任务。第二阶段
     * @param transactionId
     * @param scheduleId
     * @param follows
     * @throws Exception
     */
    public static void confirm(String transactionId, String scheduleId, List<BaseNode> follows) throws Exception {
        Iterator<BaseNode> items = follows.iterator();
        while (items.hasNext()) {
            BaseNode follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranConfirmSaveTask).setSource(NodeService.getConfig().getAddress())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret =NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if (ret) {
                ScheduleSyncDao.updateStatusByScheduleIdAndSlave(scheduleId, follow.getAddress(), ScheduleSyncStatusEnum.SYNCED);
            } else
                throw new Exception("sendSyncMsgWithCount() exception！");
        }
        TranlogScheduleDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }

    /**
     * 事务回滚阶段。
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void cancel(String transactionId,List<BaseNode> follows) throws Exception {
        TranlogScheduleDao.updateStatusById(transactionId,TransactionStatusEnum.CANCEL);//自己优先标记需回滚
        retryCancel( transactionId, follows);
    }
    public static void retryCancel(String transactionId, List<BaseNode> follows) throws Exception {
        Iterator<BaseNode> items = follows.iterator();
        while (items.hasNext()) {
            BaseNode follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranCancelSaveTask).setSource(NodeService.getConfig().getAddress())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if (ret) {
                ScheduleSyncDao.deleteByTransactionIdAndSlave(transactionId, follow.getAddress());
            } else
                throw new Exception("ret=false");
        }
    }
}
