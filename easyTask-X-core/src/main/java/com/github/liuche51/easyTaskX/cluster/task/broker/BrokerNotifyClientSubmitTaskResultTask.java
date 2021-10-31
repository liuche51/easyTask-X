package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Broker通知客户端提交的任务同步结果反馈
 * 1、每个 客户端都有单独了发送队列。每个队列配置一个单独线程实例运行
 */
public class BrokerNotifyClientSubmitTaskResultTask extends TimerTask {
    private String client;

    public BrokerNotifyClientSubmitTaskResultTask(String client) {
        this.client = client;
    }
    @Override
    public void run() {
        while (!isExit()) {
            try {
                StringListDto.StringList.Builder builder = StringListDto.StringList.newBuilder();
                for (int i = 0; i < 10; i++) {
                    SubmitTaskResult result = MasterService.WAIT_RESPONSE_TASK_RESULT.poll();
                    if (result != null) {
                        StringBuilder str = new StringBuilder(result.getId());
                        str.append(StringConstant.CHAR_SPRIT_COMMA).append(result.getStatus())
                                .append(StringConstant.CHAR_SPRIT_COMMA).append(result.getError());
                        builder.addList(str.toString());
                    }

                }
                NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BrokerNotifyClientSubmitTaskResult)
                                    .setSource(NodeService.CURRENT_NODE.getAddress()).setBodyBytes(builder.build().toByteString());//任务ID,状态,错误信息
                            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                            if (!ret) {
                                NettyMsgService.writeRpcErrorMsgToDb("Leader通知Clinets。Broker发生变更。失败！", "com.github.liuche51.easyTaskX.cluster.leader.LeaderUtil.notifyClinetChangedBroker");
                            }
                        } catch (Exception e) {
                            log.error("", e);
                        }
                    }
                });
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                TimeUnit.HOURS.sleep(NodeService.getConfig().getAdvanceConfig().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
