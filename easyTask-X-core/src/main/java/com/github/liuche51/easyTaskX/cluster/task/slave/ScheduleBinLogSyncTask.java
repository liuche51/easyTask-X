package com.github.liuche51.easyTaskX.cluster.task.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.*;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * slave同步master Schedule主表数据BinLog异步复制
 * 1、节点启动时启动，单例
 * 2、每个master一次只能有一个线程池任务去负责同步工作，防止并发导致的数据不一致性
 */
public class ScheduleBinLogSyncTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                Iterator<Map.Entry<String, MasterNode>> items = SlaveService.MASTERS.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, MasterNode> item = items.next();
                    if (!item.getValue().isBinlogSyncing()) {//保证每个master 一次只有一个异步任务同步日志。避免多线程导致SQL执行问题。
                        BrokerService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    item.getValue().setBinlogSyncing(true);
                                    requestMasterScheduleBinLogData(item.getValue(), item.getValue().getCurrentBinlogIndex());
                                } catch (Exception e) {
                                    LogUtil.error("", e);
                                } finally {
                                    item.getValue().setBinlogSyncing(false); // 防止任务异常导致同步状态无法归位，而不能继续同步
                                }
                            }
                        });

                    }


                }

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

    /**
     * 请求master binlog日志，并本地执行。
     *
     * @param master
     * @param index
     * @throws InterruptedException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static void requestMasterScheduleBinLogData(MasterNode master, Long index) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.SlaveRequestMasterGetScheduleBinlogData).setSource(BrokerService.getConfig().getAddress())
                .setBody(String.valueOf(index));
        ByteStringPack respPack = new ByteStringPack();
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, master.getClient(), BrokerService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
        if (ret) {
            List<BinlogSchedule> binlogScheduleList = JSONObject.parseArray(respPack.getRespbody().toString(), BinlogSchedule.class);
            if (binlogScheduleList == null || binlogScheduleList.size() == 0)
                return;
            Collections.sort(binlogScheduleList, Comparator.comparing(BinlogSchedule::getId));
            for (BinlogSchedule x : binlogScheduleList) {
                String sql = x.getSql().replace(DbTableName.SCHEDULE, DbTableName.SCHEDULE_BAK);
                try {
                    ScheduleBakDao.executeSql(sql);
                    MasterNode masterNode = SlaveService.MASTERS.get(master.getAddress());
                    masterNode.setCurrentBinlogIndex(x.getId());
                    //如果当前binlog新增任务是需要等待slave同步确认的。则通知master已经完成同步
                    if (ScheduleStatusEnum.UNUSE == x.getStatus() && !StringUtils.isNullOrEmpty(x.getScheduleId())) {
                        addWAIT_RESPONSE_MASTER_TASK_RESULT(master.getAddress(),new SubmitTaskResult(x.getScheduleId(), SubmitTaskResultStatusEnum.SUCCESSED));
                    }
                } catch (SQLException e) {
                    addWAIT_RESPONSE_MASTER_TASK_RESULT(master.getAddress(),new SubmitTaskResult(x.getScheduleId(), SubmitTaskResultStatusEnum.FAILED));
                    String message = e.getMessage();
                    //防止重复处理。保证接口幂等性
                    if (message != null && message.contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                        LogUtil.info("normally exception!slave sync master's ScheduleBinLog primarykey repeated.");
                    } else {
                        LogUtil.error("sql=" + x.getSql(), e);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * 将同步结果添加到反馈master的队列中
     * @param master
     * @param submitTaskResult
     */
    private static void addWAIT_RESPONSE_MASTER_TASK_RESULT(String master, SubmitTaskResult submitTaskResult) {
        LinkedBlockingQueue<SubmitTaskResult> queue = SlaveService.WAIT_RESPONSE_MASTER_TASK_RESULT.get(master);
        if (queue == null) {
            SlaveService.WAIT_RESPONSE_MASTER_TASK_RESULT.put(master, new LinkedBlockingQueue<SubmitTaskResult>(BrokerService.getConfig().getAdvanceConfig().getTaskQueueCapacity()));
            queue = SlaveService.WAIT_RESPONSE_MASTER_TASK_RESULT.get(queue);
        }
        try {
            boolean offer = queue.offer(submitTaskResult, BrokerService.getConfig().getAdvanceConfig().getTimeOut(), TimeUnit.SECONDS);//插入队列，队列满时，超时抛出异常，以便能检查到原因
            if (offer == false) {
                LogErrorUtil.writeQueueErrorMsgToDb("队列WAIT_RESPONSE_MASTER_TASK_RESULT已满.", "com.github.liuche51.easyTaskX.cluster.task.slave.ScheduleBinLogSyncTask.addWAIT_RESPONSE_MASTER_TASK_RESULT");
            }
        } catch (InterruptedException e) {
            LogUtil.error("", e);
        }
    }

    /**
     * 通知master。还没正式使用的任务已经完成同步
     *
     * @param scheduleId
     * @param master
     */
    private static void notifyMasterHasSyncUnUseTask(String scheduleId, BaseNode master) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.SlaveNotifyMasterHasSyncUnUseTask).setSource(BrokerService.getConfig().getAddress())
                    .setBody(scheduleId);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, master.getClient(), BrokerService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (!ret) {
                LogErrorUtil.writeRpcErrorMsgToDb("slave通知master。还没正式使用的任务已经完成同步。失败！", "com.github.liuche51.easyTaskX.cluster.slave.SlaveService.notifyMasterHasSyncUnUseTask");
            }
        } catch (Exception e) {
            LogUtil.error("", e);
        }
    }
}
