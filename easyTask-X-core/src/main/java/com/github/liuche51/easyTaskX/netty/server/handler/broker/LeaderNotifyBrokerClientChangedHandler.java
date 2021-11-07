package com.github.liuche51.easyTaskX.netty.server.handler.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.broker.ReDispatchToClientTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Broker响应：leader通知brokers。Client已经变动
 */
public class LeaderNotifyBrokerClientChangedHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_STRING);//type+地址
        switch (items[0]) {
            case OperationTypeEnum.ADD:
                NodeService.CURRENT_NODE.getClients().add(new BaseNode(items[1]));
                if (!MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.containsKey(items[1])) {//找到新加入的Clinet队列
                    MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.put(items[1], new LinkedBlockingQueue<SubmitTaskResult>(NodeService.getConfig().getAdvanceConfig().getWaitSubmitTaskQueueCapacity()));
                }
                break;
            case OperationTypeEnum.DELETE:
                Iterator<BaseNode> temps = NodeService.CURRENT_NODE.getClients().iterator();
                while (temps.hasNext()) {
                    BaseNode bn = temps.next();
                    if (bn.getAddress().equals(items[1])) {
                        NodeService.CURRENT_NODE.getClients().remove(bn);
                        //移除该客户端任务反馈发送队列，如果队列中有反馈的任务，则需要删除之
                        LinkedBlockingQueue<SubmitTaskResult> submitTaskResults = MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.get(items[1]);
                        List<SubmitTaskResult> all=new ArrayList<>(submitTaskResults.size());
                        if(submitTaskResults!=null&&submitTaskResults.size()>0){
                            submitTaskResults.drainTo(all,Integer.MAX_VALUE);
                        }
                        MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.remove(items[1]);
                        all.forEach(x->{
                            BrokerService.deleteTask(x.getId());
                        });
                        if (ScheduleDao.isExistByExecuter(bn.getAddress())) {
                            BrokerService.notifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus(NodeSyncDataStatusEnum.SYNCING);
                            ReDispatchToClientTask task = new ReDispatchToClientTask(bn);
                            task.start();
                        }
                    }
                }
                break;
            default:
                break;
        }
        return null;
    }
}
