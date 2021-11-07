package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Broker重新将旧Client任务分配给新Client
 * 触发条件：旧Client失效
 */
public class ReDispatchToClientTask extends OnceTask {
    private BaseNode oldClient;

    public ReDispatchToClientTask(BaseNode oldClient) {
        this.oldClient = oldClient;
    }

    @Override
    public void run() {
        synchronized (this.getClass()) { // 防止触发了多个实例运行。实现幂等性
            while (!isExit()) {
                try {
                    //获取批次数据
                    List<Schedule> list = ScheduleDao.selectByExecuter(this.oldClient.getAddress(), NodeService.getConfig().getAdvanceConfig().getReDispatchBatchCount());
                    if (list.size() == 0) {//如果已经同步完，通知leader更新注册表状态并则跳出循环
                        BrokerService.notifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus(NodeSyncDataStatusEnum.SUCCEEDED);
                        setExit(true);
                        break;
                    }
                    BaseNode newClient = findNewClient(this.oldClient);
                    boolean ret = notifyClientExecuteNewTask(newClient, list);
                    if (ret) {
                        Map<String, Object> values = new HashMap<>();
                        values.put("executer", newClient.getAddress());
                        String[] scheduleIds = list.stream().map(Schedule::getId).toArray(String[]::new);
                        ScheduleDao.updateByIds(scheduleIds, values);
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            }

        }
    }

    /**
     * Broker通知Client接受执行新任务
     *
     * @param newClient
     * @param schedules
     * @return
     * @throws Exception
     */
    private boolean notifyClientExecuteNewTask(BaseNode newClient, List<Schedule> schedules) throws Exception {
        ScheduleDto.ScheduleList.Builder builder0 = ScheduleDto.ScheduleList.newBuilder();
        for (Schedule schedule : schedules) {
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            builder0.addSchedules(s);
        }
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BrokerNotifyClientExecuteNewTask).setSource(NodeService.getConfig().getAddress())
                .setBodyBytes(builder0.build().toByteString());
        NettyClient client = newClient.getClientWithCount(1);
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
        return ret;
    }

    /**
     * 随机获取一个Client
     *
     * @return
     * @throws Exception
     */
    private BaseNode findNewClient(BaseNode oldClient) throws Exception {
        CopyOnWriteArrayList<BaseNode> clients = NodeService.CURRENT_NODE.getClients();
        BaseNode selectedNode = null;
        if (clients == null || clients.size() == 0)
            throw new Exception("clients==null||clients.size()==0");
        else if (clients.size() > 1) {
            Random random = new Random();
            int index = random.nextInt(clients.size());//随机生成的随机数范围就变成[0,size)。
            selectedNode = clients.get(index);
        } else
            selectedNode = clients.get(0);
        if (oldClient.getAddress().equals(selectedNode.getAddress())) {
            TimeUnit.SECONDS.sleep(1);//此处防止不满足条件时重复高频递归本方法
            return findNewClient(oldClient);
        }
        return selectedNode;
    }
}
