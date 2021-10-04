package com.github.liuche51.easyTaskX.cluster.task.slave;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.MasterNode;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * bakleader同步leader集群元数据BinLog任务，异步复制
 * 1、
 */
public class ClusterMetaBinLogSyncTask extends TimerTask {
    //是否已经存在一个任务实例运行中
    public static volatile boolean hasRuning = false;
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                Iterator<Map.Entry<String, MasterNode>> items = NodeService.masterBinlogInfo.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, MasterNode> item = items.next();
                    if (!item.getValue().isSyncing()) {//保证每个master 一次只有一个异步任务同步日志。避免多线程导致SQL执行问题。
                        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    item.getValue().setSyncing(true);
                                    SlaveService.requestMasterScheduleBinLogData(item.getValue(), item.getValue().getCurrentIndex());
                                } catch (Exception e) {
                                    log.error("", e);
                                } finally {
                                    item.getValue().setSyncing(false);
                                }
                            }
                        });

                    }


                }

            } catch (Exception e) {
                log.error("", e);
            }
            try {
                if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                    TimeUnit.MILLISECONDS.sleep(500L);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
