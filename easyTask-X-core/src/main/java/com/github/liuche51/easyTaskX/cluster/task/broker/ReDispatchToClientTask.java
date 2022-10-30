package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
                    List<Schedule> list = ScheduleDao.selectByExecuter(this.oldClient.getAddress(), BrokerService.getConfig().getAdvanceConfig().getReDispatchBatchCount());
                    if (list.size() == 0) {//如果已经同步完，通知leader更新注册表状态并则跳出循环
                        Map<String,Integer> map=new HashMap<>(Util.getMapInitCapacity(2));
                        map.put(StringConstant.NODESTATUS, NodeStatusEnum.NORMAL);
                        BrokerUtil.notifyLeaderChangeRegNodeStatus(map);
                        setExit(true);
                        break;
                    }
                    BaseNode newClient = findNewClient(this.oldClient);
                    boolean ret = notifyClientExecuteNewTask(newClient, list);
                    if (ret) {
                        Map<String, Object> values = new HashMap<>();
                        values.put("executer", newClient.getAddress());
                        String[] scheduleIds = list.stream().map(Schedule::getId).toArray(String[]::new);
                        MasterService.BINLOG_LAST_INDEX=ScheduleDao.updateByIds(scheduleIds, values);
                    }
                } catch (Exception e) {
                    LogUtil.error("", e);
                }
            }

        }
    }

}
