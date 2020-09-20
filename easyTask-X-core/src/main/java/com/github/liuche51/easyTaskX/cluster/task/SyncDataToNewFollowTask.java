package com.github.liuche51.easyTaskX.cluster.task;

import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.SliceLeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.SliceLeaderUtil;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleSyncDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleSyncStatusEnum;

import java.util.List;
/**
 * leader同步数据到新follow
 * 目前设计为只有一个线程同步给某个follow
 */
public class SyncDataToNewFollowTask extends OnceTask {
    private Node oldFollow;
    private Node newFollow;

    public Node getOldFollow() {
        return oldFollow;
    }

    public void setOldFollow(Node oldFollow) {
        this.oldFollow = oldFollow;
    }

    public Node getNewFollow() {
        return newFollow;
    }

    public void setNewFollow(Node newFollow) {
        this.newFollow = newFollow;
    }
    public SyncDataToNewFollowTask(Node oldFollow, Node newFollow){
        this.oldFollow=oldFollow;
        this.newFollow=newFollow;
    }
    @Override
    public void run() {
        try {
            while (!isExit()) {
                //获取批次数据
                List<ScheduleSync> list = ScheduleSyncDao.selectByFollowAndStatusWithCount(newFollow.getAddress(), ScheduleSyncStatusEnum.UNSYNC, 5);
                if (list.size() == 0) {//如果已经同步完，通知集群leader更新注册表状态并则跳出循环
                    SliceLeaderService.notifyClusterLeaderUpdateRegeditForDataStatus(newFollow.getAddress(),String.valueOf(NodeSyncDataStatusEnum.SYNC));
                    setExit(true);
                    break;
                }
                String[] ids = list.stream().distinct().map(ScheduleSync::getScheduleId).toArray(String[]::new);
                ScheduleSyncDao.updateStatusByFollowAndScheduleIds(newFollow.getAddress(), ids, ScheduleSyncStatusEnum.SYNCING);
                List<Schedule> list1 = ScheduleDao.selectByIds(ids);
                boolean ret = SliceLeaderUtil.syncDataToFollowBatch(list1, newFollow);
                if (ret)
                    ScheduleSyncDao.updateStatusByFollowAndStatus(newFollow.getAddress(), ScheduleSyncStatusEnum.SYNCING, ScheduleSyncStatusEnum.SYNCED);
            }
        } catch (Exception e) {
            log.error("syncDataToNewFollow() exception!", e);
        }
    }
}
