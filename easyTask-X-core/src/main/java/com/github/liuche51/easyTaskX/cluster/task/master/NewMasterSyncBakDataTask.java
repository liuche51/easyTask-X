package com.github.liuche51.easyTaskX.cluster.task.master;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleBak;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.List;
/**
 * 新master将旧master的备份数据同步给自己的slave
 * 后期需要考虑数据一致性
 */
public class NewMasterSyncBakDataTask extends OnceTask {
    private String oldLeaderAddress;

    public String getOldLeaderAddress() {
        return oldLeaderAddress;
    }

    public void setOldLeaderAddress(String oldLeaderAddress) {
        this.oldLeaderAddress = oldLeaderAddress;
    }

    public NewMasterSyncBakDataTask(String oldLeaderAddress) {
        this.oldLeaderAddress = oldLeaderAddress;
    }

    @Override
    public void run() {
        try {
            while (!isExit()) {
                List<ScheduleBak> baks = ScheduleBakDao.getBySourceWithCount(oldLeaderAddress, 5);
                if (baks.size() == 0) {//如果已经同步完，标记状态并则跳出循环
                    setExit(true);
                    break;
                }
                baks.forEach(x -> {
                    try {
                        Schedule schedule = Schedule.valueOf(x);
                        NodeService.submitTask(schedule);//模拟客户端重新提交任务
                        ScheduleBakDao.delete(x.getId());
                    }
                    //遇到正在选举follow时，需要休眠500毫秒。防止短时间内反复提交失败
                    catch (VotingException e){
                        log.error("normally exception!submitNewTaskByOldLeader()->"+e.getMessage());
                        try {
                            Thread.sleep(500l);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                    catch (Exception e) {
                        log.error("submitNewTaskByOldLeader()->", e);
                    }
                });
            }
        } catch (Exception e) {
            log.error("submitNewTaskByOldLeader()->", e);
        }
    }
}
