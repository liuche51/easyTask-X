package com.github.liuche51.easyTaskX.cluster.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.ScheduleBinLogSyncTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.LogErrorUtil;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Slave服务入口
 */
public class SlaveService {
    private static final Logger log = LoggerFactory.getLogger(SlaveService.class);

    /**
     * 请求master binlog日志，并本地执行。
     *
     * @param master
     * @param index
     * @throws InterruptedException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void requestMasterScheduleBinLogData(MasterNode master, Long index) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.SlaveRequestMasterGetScheduleBinlogData).setSource(NodeService.getConfig().getAddress())
                .setBody(String.valueOf(index));
        ByteStringPack respPack = new ByteStringPack();
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, master.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
        if (ret) {
            List<BinlogSchedule> binlogScheduleList = JSONObject.parseArray(respPack.getRespbody().toString(), BinlogSchedule.class);
            if (binlogScheduleList == null || binlogScheduleList.size() == 0)
                return;
            Collections.sort(binlogScheduleList, Comparator.comparing(BinlogSchedule::getId));
            for (BinlogSchedule x : binlogScheduleList) {
                String sql = x.getSql().replace(DbTableName.SCHEDULE, DbTableName.SCHEDULE_BAK);
                try {
                    ScheduleBakDao.executeSql(sql);
                    MasterNode masterNode = NodeService.masterBinlogInfo.get(master.getAddress());
                    masterNode.setCurrentIndex(x.getId());
                    //如果当前binlog新增任务是需要等待slave同步确认的。则通知master已经完成同步
                    if (ScheduleStatusEnum.UNUSE == x.getStatus() && !StringUtils.isNullOrEmpty(x.getScheduleId())) {
                        notifyMasterHasSyncUnUseTask(x.getScheduleId(), master);
                    }
                } catch (SQLException e) {
                    String message = e.getMessage();
                    //防止重复处理。保证接口幂等性
                    if (message != null && message.contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                        log.info("normally exception!slave sync master's ScheduleBinLog primarykey repeated.");
                    } else {
                        log.error("sql=" + x.getSql(), e);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * 通知master。还没正式使用的任务已经完成同步
     *
     * @param scheduleId
     * @param master
     */
    public static void notifyMasterHasSyncUnUseTask(String scheduleId, BaseNode master) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.SlaveNotifyMasterHasSyncUnUseTask).setSource(NodeService.getConfig().getAddress())
                    .setBody(scheduleId);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, master.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (!ret) {
                LogErrorUtil.writeRpcErrorMsgToDb("slave通知master。还没正式使用的任务已经完成同步。失败！", "com.github.liuche51.easyTaskX.cluster.slave.SlaveService.notifyMasterHasSyncUnUseTask");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }


    /**
     * 启动从master获取ScheduleBinLog订阅任务。
     */
    public static TimerTask startScheduleBinLogSyncTask() {
        ScheduleBinLogSyncTask task = new ScheduleBinLogSyncTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }
}
