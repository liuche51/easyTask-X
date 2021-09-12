package com.github.liuche51.easyTaskX.cluster.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;

import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.*;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteTaskTCC {
    private static Logger log = LoggerFactory.getLogger(DeleteTaskTCC.class);
    /**
     * 删除任务事务第一阶段。
     * 先记入事务表
     * @param taskId  任务ID
     * @param slaves
     * @throws Exception
     */
    public static void tryDel(String transactionId,String taskId, List<BaseNode> slaves) throws Exception {
        List<String> cancelHost=slaves.stream().map(BaseNode::getAddress).collect(Collectors.toList());
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setContent(taskId);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.DELETE);
        transactionLog.setSlaves(JSONObject.toJSONString(cancelHost));
        TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        try {
            retryDel( transactionId, taskId,  slaves);
        }catch (Exception e){
            //通知slaves删除时异常，可视为删除成功，只要本节点本地已经写了删除事务日志。后续会重试删除
            log.error("", e);
        }

    }
    public static void retryDel(String transactionId,String taskId, List<BaseNode> slaves) throws Exception {
        //需要将同步记录表原来的提交已同步记录修改为删除中，并更新其事务ID
        ScheduleSyncDao.updateStatusAndTransactionIdByScheduleId(taskId,ScheduleSyncStatusEnum.DELETEING, transactionId);
        Iterator<BaseNode> items = slaves.iterator();
        while (items.hasNext()) {
            BaseNode follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranTryDelTask).setSource(NodeService.getConfig().getAddress())
                    .setBody(transactionId+ StringConstant.CHAR_SPRIT_STRING +taskId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if(!ret){
                throw new Exception("ret=false");
            }
        }
        //删除操作，如果slave都被通知标记为TRIED了，就不走后面的第二阶段CONFIRM了，可以直接删除任务。只需要master标记即可
        TranlogScheduleDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }
    /**
     * 确认删除任务。第二阶段
     * 暂时不需要实现。因为对于删除操作来说是不可逆的，不需要回滚。只要第一阶段被标记为TRIED了就可以直接删除了
     * @param transactionId
     * @param slaves
     * @throws Exception
     */
    @Deprecated
    public static void confirm(String transactionId, List<Node> slaves) throws Exception {

    }

    /**
     * 事务回滚阶段。
     * 暂时不需要实现。删除任务回滚没有意义。使用最大努力通知方式，确认标记删除
     * @param transactionId
     * @param slaves
     * @throws Exception
     */
    @Deprecated
    public static void cancel(String transactionId,List<Node> slaves) throws Exception {

    }
}
