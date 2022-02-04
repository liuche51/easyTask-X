package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.BakLeaderService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MasterUpdateDataStatusTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {

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
