package com.github.liuche51.easyTaskX.cluster.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.Node;

import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TransactionLogDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
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
    public static void trySave(String transactionId,Schedule schedule, List<Node> follows) throws Exception {
        List<String> cancelHost=follows.stream().map(Node::getAddress).collect(Collectors.toList());
        schedule.setTransactionId(transactionId);
        schedule.setSource(Util.getSource(schedule.getSource()));
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setContent(JSONObject.toJSONString(schedule));
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.SAVE);
        transactionLog.setFollows(JSONObject.toJSONString(cancelHost));
        TransactionLogDao.saveBatch(Arrays.asList(transactionLog));
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            ScheduleSync scheduleSync = new ScheduleSync();
            scheduleSync.setTransactionId(transactionLog.getId());
            scheduleSync.setScheduleId(schedule.getId());
            scheduleSync.setFollows(follow.getAddress());
            scheduleSync.setStatus(ScheduleSyncStatusEnum.SYNCING);
            ScheduleSyncDao.save(scheduleSync);//记录同步状态表
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_TRYSAVETASK).setSource(NodeService.getConfig().getAddress())
                    .setBodyBytes(s.toByteString());
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if(!ret){
                throw new Exception("sendSyncMsgWithCount()->exception! ");
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
    public static void confirm(String transactionId, String scheduleId, List<Node> follows) throws Exception {
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_CONFIRMSAVETASK).setSource(NodeService.getConfig().getAddress())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret =NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if (ret) {
                ScheduleSyncDao.updateStatusByScheduleIdAndFollow(scheduleId, follow.getAddress(), ScheduleSyncStatusEnum.SYNCED);
            } else
                throw new Exception("sendSyncMsgWithCount() exception！");
        }
        TransactionLogDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }

    /**
     * 事务回滚阶段。
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void cancel(String transactionId,List<Node> follows) throws Exception {
        TransactionLogDao.updateStatusById(transactionId,TransactionStatusEnum.CANCEL);//自己优先标记需回滚
        retryCancel( transactionId, follows);
    }
    public static void retryCancel(String transactionId, List<Node> follows) throws Exception {
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_CANCELSAVETASK).setSource(NodeService.getConfig().getAddress())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if (ret) {
                ScheduleSyncDao.deleteByTransactionIdAndFollow(transactionId, follow.getAddress());
            } else
                throw new Exception("sendSyncMsgWithCount() exception！");
        }
    }
}
