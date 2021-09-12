package com.github.liuche51.easyTaskX.cluster.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.TransactionLog;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.*;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateTaskTCC {
    private static Logger log = LoggerFactory.getLogger(UpdateTaskTCC.class);
    /**
     * 更新任务事务第一阶段。
     * 先记入事务表
     * @param taskIds  任务ID
     * @param slaves
     * @param values 修改了字段值
     * @throws Exception
     */
    public static void tryUpdate(String transactionId, String[] taskIds, List<BaseNode> slaves, Map<String,String> values) throws Exception {
        List<String> cancelHost=slaves.stream().map(BaseNode::getAddress).collect(Collectors.toList());
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        String json=JSONObject.toJSONString(values);
        String taskIds2=String.join(",",taskIds);
        transactionLog.setContent(taskIds2+ StringConstant.CHAR_SPRIT_STRING+json);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.UPDATE);
        transactionLog.setSlaves(JSONObject.toJSONString(cancelHost));
        TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        try {
            retryUpdate( transactionId, taskIds2,  slaves,json);
        }catch (Exception e){
            //通知slaves更新时异常，可视为更新成功，只要本节点本地已经写了更新事务日志。后续会重试更新
            log.error("", e);
        }

    }
    public static void retryUpdate(String transactionId,String taskIds, List<BaseNode> slaves, String values) throws Exception {
        Iterator<BaseNode> items = slaves.iterator();
        while (items.hasNext()) {
            BaseNode follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranTryUpdateTask).setSource(NodeService.getConfig().getAddress())
                    .setBody(transactionId+StringConstant.CHAR_SPRIT_STRING+taskIds+StringConstant.CHAR_SPRIT_STRING+values);//
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder,client,1,0,null);
            if(!ret){
                throw new Exception("ret=false");
            }
        }
        //更新操作，如果slave都被通知标记为TRIED了，就不走后面的第二阶段CONFIRM了，可以直接更新任务。只需要master标记即可
        TranlogScheduleDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }
    /**
     * 确认更新任务。第二阶段
     * 暂时不需要实现。因为对于更新操作来说是不可逆的，不需要回滚。只要第一阶段被标记为TRIED了就可以直接更新了
     * @param transactionId
     * @param slaves
     * @throws Exception
     */
    @Deprecated
    public static void confirm(String transactionId, List<Node> slaves) throws Exception {

    }

    /**
     * 事务回滚阶段。
     * 暂时不需要实现。更新任务回滚没有意义。使用最大努力通知方式，确认标记更新
     * @param transactionId
     * @param slaves
     * @throws Exception
     */
    @Deprecated
    public static void cancel(String transactionId,List<Node> slaves) throws Exception {

    }
}
