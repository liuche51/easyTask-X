package com.github.liuche51.easyTaskX.cluster.task.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.db.TranlogScheduleBak;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * slave同步master Schedule主表数据BinLog异步复制
 * 1、参与tcc的slave节点，虽然新增任务都是和master同步的。但是考虑到通用情况，异步复制binlog重新新增任务也无大碍。顶多报个主键重复错误即可。不影响数据一致性
 */
public class ScheduleBinLogSyncTask extends TimerTask {
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
