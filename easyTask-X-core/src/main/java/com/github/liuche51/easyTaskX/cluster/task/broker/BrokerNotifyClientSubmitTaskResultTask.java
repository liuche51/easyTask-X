package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.LogErrorUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Broker通知客户端提交的任务同步结果反馈
 * 1、每个客户端都有单独了发送队列。只有一个任务运行，轮询它们
 */
public class BrokerNotifyClientSubmitTaskResultTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                List<SubmitTaskResult> results = new ArrayList<>(10);
                Iterator<Map.Entry<String, LinkedBlockingQueue<SubmitTaskResult>>> items = MasterService.WAIT_RESPONSE_CLINET_TASK_RESULT.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, LinkedBlockingQueue<SubmitTaskResult>> item = items.next();
                    item.getValue().drainTo(results, 10);
                    if (results.size() > 0) {
                        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    StringListDto.StringList.Builder builder0 = StringListDto.StringList.newBuilder();
                                    results.forEach(x -> {
                                        StringBuilder str = new StringBuilder(x.getId());
                                        str.append(StringConstant.CHAR_SPRIT_COMMA).append(x.getStatus())
                                                .append(StringConstant.CHAR_SPRIT_COMMA).append(x.getError());
                                        builder0.addList(str.toString());
                                    });
                                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BrokerNotifyClientSubmitTaskResult)
                                            .setSource(NodeService.CURRENT_NODE.getAddress()).setBodyBytes(builder0.build().toByteString());//任务ID,状态,错误信息
                                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, new BaseNode(item.getKey()).getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                                    if (!ret) {
                                        LogErrorUtil.writeRpcErrorMsgToDb("Leader通知Clinets。Broker发生变更。失败！", "com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil.notifyClinetChangedBroker");
                                    }
                                } catch (Exception e) {
                                    log.error("", e);
                                }

                            }
                        });
                    }
                }
                try {
                    if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                        TimeUnit.MILLISECONDS.sleep(500L);
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            } catch (Exception e) {
                log.error("", e);
            }

        }
    }
}
