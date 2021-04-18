package com.github.liuche51.easyTaskX.cluster.task.master;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.Schedule;
import com.github.liuche51.easyTaskX.dto.ScheduleBak;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 新master将旧master的备份数据同步给自己的slave
 * 后期需要考虑数据一致性
 */
public class NewMasterSyncBakDataTask extends OnceTask {
    private String oldMasterAddress;
    /**
     * 当前正在运行的Task实例。
     * 需要保证不能重复启动相同的任务检查。
     */
    public static ConcurrentHashMap<String, Object> runningTask = new ConcurrentHashMap<>();
    public NewMasterSyncBakDataTask(String oldMasterAddress) {
        this.oldMasterAddress = oldMasterAddress;
    }

    @Override
    public void run() {
        try {
            while (!isExit()) {
                List<ScheduleBak> baks = ScheduleBakDao.getBySourceWithCount(oldMasterAddress, 5);
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
                        log.info("normally exception!NewMasterSyncBakDataTask() faied."+e.getMessage());
                        try {
                            Thread.sleep(500l);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                    catch (Exception e) {
                        log.error("", e);
                    }
                });
            }
            runningTask.remove(this.getClass().getName() + "," + this.oldMasterAddress);
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
