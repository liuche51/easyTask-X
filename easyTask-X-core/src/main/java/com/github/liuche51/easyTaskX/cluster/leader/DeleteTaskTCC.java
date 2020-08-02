package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterUtil;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.core.AnnularQueue;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dao.TransactionLogDao;
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
     * @param follows
     * @throws Exception
     */
    public static void tryDel(String transactionId,String taskId, List<Node> follows) throws Exception {
        List<String> cancelHost=follows.stream().map(Node::getAddress).collect(Collectors.toList());
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setContent(taskId);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.DELETE);
        transactionLog.setFollows(JSONObject.toJSONString(cancelHost));
        TransactionLogDao.saveBatch(Arrays.asList(transactionLog));
        try {
            retryDel( transactionId, taskId,  follows);
        }catch (Exception e){
            //通知follow删除时异常，可视为删除成功，只要本节点本地已经写了删除事务日志。后续会重试删除
            log.error("retryDel()-> exception!", e);
        }

    }
    public static void retryDel(String transactionId,String taskId, List<Node> follows) throws Exception {
        //需要将同步记录表原来的提交已同步记录修改为删除中，并更新其事务ID
        ScheduleSyncDao.updateStatusAndTransactionIdByScheduleId(taskId,ScheduleSyncStatusEnum.DELETEING, transactionId);
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_TRYDELTASK).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(transactionId+","+taskId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if(!ret){
                throw new Exception("tryDel->sendSyncMsgWithCount():exception! ");
            }
        }
        //删除操作，如果follow都被通知标记为TRIED了，就不走后面的第二阶段CONFIRM了，可以直接删除任务。只需要leader标记即可
        TransactionLogDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }
    /**
     * 确认删除任务。第二阶段
     * 暂时不需要实现。因为对于删除操作来说是不可逆的，不需要回滚。只要第一阶段被标记为TRIED了就可以直接删除了
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void confirm(String transactionId, List<Node> follows) throws Exception {
       /* Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_CONFIRMDELTASK).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if (ret) {
                //follow已经标记为确认删除了，就可以认为是已经完成删除
                ScheduleSyncDao.updateStatusByTransactionIdAndFollow(transactionId, follow.getAddress(), ScheduleSyncStatusEnum.DELETED);
            } else
                throw new Exception("confirm()->sendSyncMsgWithCount() exception！");
        }
        //follow都标记为确认删除了。就可以认为数据已经删除，后面不需要重试标记确认删除了。否则后面还需要重试本方法确认删除
        TransactionDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);*/
    }

    /**
     * 事务回滚阶段。
     * 暂时不需要实现。删除任务回滚没有意义。使用最大努力通知方式，确认标记删除
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void cancel(String transactionId,List<Node> follows) throws Exception {

    }
}
