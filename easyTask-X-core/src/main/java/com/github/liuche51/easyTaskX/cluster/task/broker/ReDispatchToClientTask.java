package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.master.MasterUtil;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;

import java.util.*;

/**
 * Broker重新将旧Client任务分配给新Client
 * 触发条件：旧Client失效
 */
public class ReDispatchToClientTask extends OnceTask {
    private Node oldClient;
    private Node newClient;
    public ReDispatchToClientTask(Node oldClient,Node newClient){
        this.oldClient=oldClient;
        this.newClient=newClient;
    }
    @Override
    public void run() {
        try {
            while (!isExit()) {
                //获取批次数据
                List<String> list = ScheduleDao.selectIdsByExecuter(this.oldClient.getAddress(),5);
                if (list.size() == 0) {//如果已经同步完，通知leader更新注册表状态并则跳出循环
                    //MasterService.notifyClusterLeaderUpdateRegeditForDataStatus(newSlave.getAddress(),String.valueOf(NodeSyncDataStatusEnum.SYNC));
                    setExit(true);
                    break;
                }
                List<BaseNode> objHost=new LinkedList<>();
                objHost.addAll((Collection<? extends BaseNode>) NodeService.CURRENTNODE.getSlaves());
                objHost.add(newClient);
                if(NodeService.canAllConnect(objHost)){
                    Map<String,String> values=new HashMap<>();
                    values.put("executer",newClient.getAddress());
                    NodeService.updateTask(list.toArray(new String[list.size()]),values);
                }

            }
        } catch (Exception e) {
            log.error("syncDataToNewFollow() exception!", e);
        }
    }
}
