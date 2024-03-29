package com.github.liuche51.easyTaskX.cluster.task.leader;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.leader.BakLeaderService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.util.LogUtil;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * bakleader同步leader集群元数据BinLog任务，异步复制
 * 1、每个bakleader都运行一个任务实例
 */
public class ClusterMetaBinLogSyncTask extends TimerTask {
    //是否已经存在一个任务实例运行中
    public static volatile boolean hasRuning = false;
    /**
     * 当前已经同步日志的位置号。默认0，表示未开始
     */
    private long currentIndex = 0;

    public long getCurrentIndex() {
        return currentIndex;
    }

    public void setCurrentIndex(long currentIndex) {
        this.currentIndex = currentIndex;
    }

    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                BaseNode leader = BrokerService.CLUSTER_LEADER;
                BakLeaderService.requestLeaderSyncClusterMetaData(leader, this);
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                    TimeUnit.MILLISECONDS.sleep(500L);
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }
}
