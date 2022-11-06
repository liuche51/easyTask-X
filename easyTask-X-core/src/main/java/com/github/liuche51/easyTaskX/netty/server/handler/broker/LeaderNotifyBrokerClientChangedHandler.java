package com.github.liuche51.easyTaskX.netty.server.handler.broker;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NodeStatusEnum;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.google.protobuf.ByteString;

import java.util.*;
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
            case OperationTypeEnum.ADD: // 这里无需添加 WAIT_RESPONSE_CLINET_TASK_RESULT队列，有实际数据添加到队列时，再新增队列
                BrokerService.CLIENTS.add(new BaseNode(items[1]));
                break;
            case OperationTypeEnum.DELETE:
                Iterator<BaseNode> temps = BrokerService.CLIENTS.iterator();
                while (temps.hasNext()) {
                    BaseNode bn = temps.next();
                    if (bn.getAddress().equals(items[1])) {
                        BrokerService.CLIENTS.remove(bn);
                        //移除该客户端任务反馈发送队列，如果队列中有反馈的任务，则需要删除之
                        LinkedBlockingQueue<SubmitTaskResult> submitTaskResults = MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.get(items[1]);
                        List<SubmitTaskResult> all = new ArrayList<>(submitTaskResults.size());
                        if (submitTaskResults != null && submitTaskResults.size() > 0) {
                            submitTaskResults.drainTo(all, Integer.MAX_VALUE);
                        }
                        MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.remove(items[1]);
                        all.forEach(x -> {
                            MasterService.addWAIT_DELETE_TASK(x.getId());
                        });
                    }
                }
                break;
            default:
                break;
        }
        return null;
    }
}
