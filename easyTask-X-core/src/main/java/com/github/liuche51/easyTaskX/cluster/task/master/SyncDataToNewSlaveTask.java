package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.master.MasterUtil;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * master同步数据到新slave
 * 目前设计为只有一个线程同步给某个slave
 */
public class SyncDataToNewSlaveTask extends OnceTask {
    private Node oldSlave;
    private Node newSlave;
    /**
     * 当前正在运行的Task实例。
     * 需要保证不能重复启动相同的任务检查。
     */
    public static ConcurrentHashMap<String, Object> runningTask = new ConcurrentHashMap<>();
    public SyncDataToNewSlaveTask(Node oldSlave, Node newSlave){
        this.oldSlave=oldSlave;
        this.newSlave=newSlave;
    }
    @Override
    public void run() {
        try {
            while (!isExit()) {
                //获取批次数据
                List<ScheduleSync> list = ScheduleSyncDao.selectBySlaveAndStatusWithCount(newSlave.getAddress(), ScheduleSyncStatusEnum.UNSYNC, 5);
                if (list.size() == 0) {//如果已经同步完，通知leader更新注册表状态并则跳出循环
                    MasterService.notifyClusterLeaderUpdateRegeditForDataStatus(newSlave.getAddress(),String.valueOf(NodeSyncDataStatusEnum.SUCCEEDED));
                    setExit(true);
                    break;
                }
                String[] ids = list.stream().distinct().map(ScheduleSync::getScheduleId).toArray(String[]::new);
                ScheduleSyncDao.updateStatusByFollowAndScheduleIds(newSlave.getAddress(), ids, ScheduleSyncStatusEnum.SYNCING);
                List<Schedule> list1 = ScheduleDao.selectByIds(ids);
                boolean ret = MasterUtil.syncDataToFollowBatch(list1, newSlave);
                if (ret)
                    ScheduleSyncDao.updateStatusBySlaveAndStatus(newSlave.getAddress(), ScheduleSyncStatusEnum.SYNCING, ScheduleSyncStatusEnum.SYNCED);
            }
            runningTask.remove(this.getClass().getName() + "," + this.oldSlave.getAddress());
        } catch (Exception e) {
            log.error("syncDataToNewFollow() exception!", e);
        }
    }
}
