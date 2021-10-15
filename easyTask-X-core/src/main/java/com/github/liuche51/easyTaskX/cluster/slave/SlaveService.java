package com.github.liuche51.easyTaskX.cluster.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.broker.BrokerUpdateClientsTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.ClusterMetaBinLogSyncTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.ScheduleBinLogSyncTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.db.TranlogScheduleBak;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Slave服务入口
 */
public class SlaveService {
    private static final Logger log = LoggerFactory.getLogger(SlaveService.class);

    /**
     * 接受Master同步任务入备库
     *
     * @param schedule
     */
    public static void trySaveTask(ScheduleDto.Schedule schedule) throws Exception {
        ScheduleBak bak = ScheduleBak.valueOf(schedule);
        TranlogScheduleBak transactionLog = new TranlogScheduleBak();
        transactionLog.setId(schedule.getTransactionId());
        transactionLog.setContent(JSONObject.toJSONString(bak));
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        TranlogScheduleBakDao.saveBatch(Arrays.asList(transactionLog));
    }

    /**
     * 确认提交任务备份
     *
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void confirmSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TranlogScheduleBakDao.updateStatusById(transactionId, TransactionStatusEnum.CONFIRM);
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
    public static void requestMasterScheduleBinLogData(MasterNode master, Long index) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.SlaveRequestMasterGetScheduleBinlogData).setSource(NodeService.getConfig().getAddress())
                .setBody(String.valueOf(index));
        ByteStringPack respPack = new ByteStringPack();
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, master.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
        if (ret) {
            List<BinlogSchedule> binlogScheduleList = JSONObject.parseArray(respPack.getRespbody().toString(), BinlogSchedule.class);
            if (binlogScheduleList == null || binlogScheduleList.size() == 0) return;
            Collections.sort(binlogScheduleList, Comparator.comparing(BinlogSchedule::getId));
            for (BinlogSchedule x : binlogScheduleList) {
                String sql = x.getSql().replace(DbTableName.SCHEDULE, DbTableName.SCHEDULE_BAK);
                try {
                    ScheduleBakDao.executeSql(sql);
                    MasterNode masterNode = NodeService.masterBinlogInfo.get(master.getAddress());
                    masterNode.setCurrentIndex(x.getId());
                } catch (SQLException e) {
                    String message = e.getMessage();
                    //因为开启了数据不丢失模式，导致其中一个slave 通过tcc机制已经与master的新增任务保持一致了，异步复制binlog时会导致主键冲突。故需要忽略此类冲突
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
     * 请求leader获取集群元数据
     *
     * @param leader
     * @param task   当前运行的任务
     * @throws Exception
     */
    public static void requestLeaderSyncClusterMetaData(BaseNode leader, ClusterMetaBinLogSyncTask task) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BakLeaderRequestLeaderGetClusterMetaBinlogData).setSource(NodeService.getConfig().getAddress())
                .setBody(String.valueOf(task.getCurrentIndex()));
        ByteStringPack respPack = new ByteStringPack();
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, leader.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
        if (ret) {
            List<BinlogClusterMeta> binlogClusterMetas = JSONObject.parseArray(respPack.getRespbody().toString(), BinlogClusterMeta.class);
            if (binlogClusterMetas == null || binlogClusterMetas.size() == 0) return;
            Collections.sort(binlogClusterMetas, Comparator.comparing(BinlogClusterMeta::getId));
            for (BinlogClusterMeta x : binlogClusterMetas) {
                Long id = SlaveUtil.saveBinlogClusterMeta(x);
                if (id != null) // id为null表示当前处理的是心跳类数据，不需要保存处理位置
                    task.setCurrentIndex(id);
            }
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
