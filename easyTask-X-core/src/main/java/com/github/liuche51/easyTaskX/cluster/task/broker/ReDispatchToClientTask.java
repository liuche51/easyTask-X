package com.github.liuche51.easyTaskX.cluster.task.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.master.MasterUtil;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

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
                List<Schedule> list = ScheduleDao.selectByExecuter(this.oldClient.getAddress(),NodeService.getConfig().getAdvanceConfig().getReDispatchBatchCount());
                if (list.size() == 0) {//如果已经同步完，通知leader更新注册表状态并则跳出循环
                    //MasterService.notifyClusterLeaderUpdateRegeditForDataStatus(newSlave.getAddress(),String.valueOf(NodeSyncDataStatusEnum.SYNC));
                    setExit(true);
                    break;
                }
                List<BaseNode> objHost=new LinkedList<>();
                objHost.addAll((Collection<? extends BaseNode>) NodeService.CURRENTNODE.getSlaves());
                objHost.add(newClient);
                if(NodeService.canAllConnect(objHost)){
                    BaseNode newClient = findNewClient(this.oldClient);
                    boolean ret = BrokerService.notifyClientExecuteNewTask(newClient, list);
                    if(ret){
                        Map<String,String> values=new HashMap<>();
                        values.put("executer", this.newClient.getAddress());
                        String[] scheduleIds=list.stream().map(Schedule::getId).toArray(String[]::new);
                        NodeService.updateTask(scheduleIds,values);
                    }

                }

            }
        } catch (Exception e) {
            log.error("syncDataToNewFollow() exception!", e);
        }
    }

    /**
     * 随机获取一个Client
     * @return
     * @throws Exception
     */
    private BaseNode findNewClient(Node oldClient) throws Exception {
        CopyOnWriteArrayList<BaseNode> clients= NodeService.CURRENTNODE.getClients();
        BaseNode selectedNode = null;
        if(clients==null||clients.size()==0)
            throw new Exception("clients==null||clients.size()==0");
        else if (clients.size() > 1) {
            Random random = new Random();
            int index = random.nextInt(clients.size());//随机生成的随机数范围就变成[0,size)。
            selectedNode=clients.get(index);
        } else
            selectedNode = clients.get(0);
        if(oldClient.getAddress().equals(selectedNode.getAddress())){
            TimeUnit.SECONDS.sleep(1);//此处防止不满足条件时重复高频递归本方法
            return findNewClient(oldClient);
        }
        return selectedNode;
    }
}
