package com.github.liuche51.easyTaskX.cluster.task.master;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.enume.DataStatusEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * master上报leader当前其slave数据状态
 * 1、防止频繁推送影响性能
 * 2、定时重推，提升容错性
 */
public class MasterUpdateSlaveDataStatusTask extends TimerTask {
    //最后一次强制推送时间
    private static ZonedDateTime lastForceNotifyTime = ZonedDateTime.now();

    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                MasterService.SLAVES.values().forEach(x -> {
                    int datastatus = DataStatusEnum.NORMAL;
                    //如果当前slave的binlog同步位置落后于master最新的日志位置为配置的批量大小值，说明没有及时同步，上报leader。状态改为未完成同步
                    if (MasterService.BINLOG_LAST_INDEX - x.getCurrentBinlogIndex() >= BrokerService.getConfig().getAdvanceConfig().getBinlogCount()) {
                        datastatus = DataStatusEnum.UNSYNC;
                    }
                    //只有最有一次状态和当前不同时或者当前距离上次强推超过5分钟时才推送给leader标记。
                    if (x.getLastDataStatus() != datastatus || ZonedDateTime.now().minusMinutes(5).isAfter(lastForceNotifyTime)) {
                        x.setLastDataStatus(datastatus);
                        Map<String, Integer> map = new HashMap<>(Util.getMapInitCapacity(1));
                        map.put(StringConstant.DATASTATUS, datastatus);
                        BrokerUtil.notifyLeaderChangeRegNodeStatus(map);
                        lastForceNotifyTime = ZonedDateTime.now();
                    }

                });
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                if (new Date().getTime() - getLastRunTime().getTime() < 1000L)//防止频繁空转
                    TimeUnit.MILLISECONDS.sleep(1000L);
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }
}
