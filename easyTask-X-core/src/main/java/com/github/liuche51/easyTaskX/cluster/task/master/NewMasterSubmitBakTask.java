package com.github.liuche51.easyTaskX.cluster.task.master;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.OnceTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.exception.VotingException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 新master将旧master的备份任务重新提交给自己
 */
public class NewMasterSubmitBakTask extends OnceTask {
    // 失效master地址
    private String oldMasterAddress;

    public NewMasterSubmitBakTask(String oldMasterAddress) {
        this.oldMasterAddress = oldMasterAddress;
    }

    @Override
    public void run() {
        synchronized (this.getClass()) { // 防止触发了多个实例运行。实现幂等性
            try {
                while (!isExit()) {
                    List<ScheduleBak> baks = ScheduleBakDao.getBySourceWithCount(oldMasterAddress, 5);
                    if (baks.size() == 0) {//如果已经同步完，标记状态并则跳出循环
                        setExit(true);
                        break;
                    }
                    List<String> ids = new ArrayList<>(baks.size());
                    baks.forEach(x -> {
                        ids.add(x.getId());
                        Schedule schedule = Schedule.valueOf(x);
                        schedule.setSubmit_model(1);//普通模式
                        try {
                            MasterService.WAIT_SUBMIT_TASK.put(schedule);//模拟客户端重新提交任务。异常情况使用阻塞接口
                        } catch (InterruptedException e) {
                            LogUtil.error("", e);
                        }
                    });
                    ScheduleBakDao.deleteAll();
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
        }
    }
}
