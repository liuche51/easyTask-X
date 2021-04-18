package com.github.liuche51.easyTaskX.cluster.slave;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.slave.BakLeaderRequestUpdateRegeditTask;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.TransactionLogDao;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Follow服务入口
 */
public class SlaveService {
    private static final Logger log = LoggerFactory.getLogger(MasterService.class);

    /**
     * 接受leader同步任务入备库
     *
     * @param schedule
     */
    public static void trySaveTask(ScheduleDto.Schedule schedule) throws Exception {
        ScheduleBak bak = ScheduleBak.valueOf(schedule);
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(schedule.getTransactionId());
        transactionLog.setContent(JSONObject.toJSONString(bak));
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.SAVE);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
        transactionLog.setSlaves(StringConstant.EMPTY);
        TransactionLogDao.saveBatch(Arrays.asList(transactionLog));
    }

    /**
     * 确认提交任务备份
     *
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void confirmSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TransactionLogDao.updateStatusById(transactionId, TransactionStatusEnum.CONFIRM);
    }

    /**
     * 取消备份任务
     *
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void cancelSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TransactionLogDao.updateStatusById(transactionId, TransactionStatusEnum.CANCEL);
    }

    /**
     * 接受leader同步删除任务
     * 本地环境偶尔会出现多次重复瞬时调用现象。导致transactionId冲突了。目前认为是Netty重试造成的。暂不需要加锁处理，
     */
    public static void tryDelTask(String transactionId, String scheduleId) throws Exception {
        System.out.println(DateUtils.getCurrentDateTime() + "  transactionId=" + transactionId + " scheduleId=" + scheduleId);
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setContent(scheduleId);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.DELETE);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
        try {
            TransactionLogDao.saveBatch(Arrays.asList(transactionLog));
        } catch (SQLiteException e) {
            //如果遇到主键冲突异常，则略过。主要原因是Netty重试造成，不影响系统功能
            if (e.getMessage() != null && e.getMessage().contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                log.info("tryDelTask():transactionId=" + transactionId + " scheduleId=" + scheduleId);
                log.error("normally exception!! tryDelTask():" + e.getMessage());
            }
        }

    }
    /**
     * 接受leader同步更新任务
     * 本地环境偶尔会出现多次重复瞬时调用现象。导致transactionId冲突了。目前认为是Netty重试造成的。暂不需要加锁处理，
     */
    public static void tryUpdateTask(String transactionId, String taskIds,String values) throws Exception {
        System.out.println(DateUtils.getCurrentDateTime() + "  transactionId=" + transactionId + " taskIds=" + taskIds);
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setContent(taskIds+StringConstant.CHAR_SPRIT_STRING+values);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.UPDATE);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
        try {
            TransactionLogDao.saveBatch(Arrays.asList(transactionLog));
        } catch (SQLiteException e) {
            //如果遇到主键冲突异常，则略过。主要原因是Netty重试造成，不影响系统功能
            if (e.getMessage() != null && e.getMessage().contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                log.info("tryDelTask():transactionId=" + transactionId + " taskIds=" + taskIds);
                log.error("normally exception!! tryUpdateTask():" + e.getMessage());
            }
        }

    }
    /**
     * 接受leader批量同步任务入备库
     *
     * @param scheduleList
     */
    public static void saveScheduleBakBatchByTran(ScheduleDto.ScheduleList scheduleList) throws Exception {
        List<ScheduleDto.Schedule> list = scheduleList.getSchedulesList();
        if (list == null) return;
        List<TransactionLog> logs = new ArrayList<>(list.size());
        list.forEach(x -> {
            ScheduleBak bak = ScheduleBak.valueOf(x);
            TransactionLog transactionLog = new TransactionLog();
            transactionLog.setId(bak.getTransactionId());
            transactionLog.setContent(JSONObject.toJSONString(bak));
            transactionLog.setStatus(TransactionStatusEnum.CONFIRM);
            transactionLog.setType(TransactionTypeEnum.SAVE);
            transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
            transactionLog.setSlaves(StringConstant.EMPTY);
            logs.add(transactionLog);
        });
        try {
            TransactionLogDao.saveBatch(logs);
        } catch (SQLiteException e) {
            //如果遇到主键冲突异常，则略过。主要原因是Netty重试造成，不影响系统功能
            if (e.getMessage() != null && e.getMessage().contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                log.error("normally exception!! tryDelTask():" + e.getMessage());
            }
        }
    }
    /**
     * 集群Salve定时从leader获取注册表最新信息
     * 覆盖本地信息
     *
     * @return
     */
    public static void requestUpdateClusterRegedit() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BakLeaderRequestLeaderSendRegedit).setSource(NodeService.getConfig().getAddress());
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                String body=respPack.getRespbody().toStringUtf8();
                String[] items=body.split(StringConstant.CHAR_SPRIT_STRING);
                ConcurrentHashMap<String, RegBroker> broker= JSONObject.parseObject(items[0], new TypeReference<ConcurrentHashMap<String, RegBroker>>(){});
                ConcurrentHashMap<String, RegClient> clinet= JSONObject.parseObject(items[1], new TypeReference<ConcurrentHashMap<String, RegClient>>(){});
                LeaderService.BROKER_REGISTER_CENTER=broker;
                LeaderService.CLIENT_REGISTER_CENTER=clinet;
            } else {
                log.info("normally exception!requestUpdateClusterRegedit() failed.");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }
    /**
     * 启动BakLeader主动通过定时任务从leader更新注册表
     */
    public static TimerTask startBakLeaderRequestUpdateRegeditTask() {
        BakLeaderRequestUpdateRegeditTask task = new BakLeaderRequestUpdateRegeditTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }
}
