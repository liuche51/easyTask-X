package com.github.liuche51.easyTaskX.cluster.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.*;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTableEnum;
import com.github.liuche51.easyTaskX.enume.TransactionTypeEnum;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(schedule.getTransactionId());
        transactionLog.setContent(JSONObject.toJSONString(bak));
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.SAVE);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
        transactionLog.setSlaves(StringConstant.EMPTY);
        TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
    }

    /**
     * 确认提交任务备份
     *
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void confirmSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TranlogScheduleDao.updateStatusById(transactionId, TransactionStatusEnum.CONFIRM);
    }

    /**
     * 取消备份任务
     *
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void cancelSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TranlogScheduleDao.updateStatusById(transactionId, TransactionStatusEnum.CANCEL);
    }

    /**
     * 接受master同步删除任务
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
            TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        } catch (SQLiteException e) {
            //如果遇到主键冲突异常，则略过。主要原因是Netty重试造成，不影响系统功能
            if (e.getMessage() != null && e.getMessage().contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                log.info("tryDelTask():transactionId=" + transactionId + " scheduleId=" + scheduleId);
                log.info("normally exception!! tryDelTask():" + e.getMessage());
            }
        }

    }
    /**
     * 接受master同步更新任务
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
            TranlogScheduleDao.saveBatch(Arrays.asList(transactionLog));
        } catch (SQLiteException e) {
            //如果遇到主键冲突异常，则略过。主要原因是Netty重试造成，不影响系统功能
            if (e.getMessage() != null && e.getMessage().contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                log.info("tryDelTask():transactionId=" + transactionId + " taskIds=" + taskIds);
                log.info("normally exception!! tryUpdateTask():" + e.getMessage());
            }
        }

    }
    /**
     * 接受master批量同步任务入备库
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
            TranlogScheduleDao.saveBatch(logs);
        } catch (SQLiteException e) {
            //如果遇到主键冲突异常，则略过。主要原因是Netty重试造成，不影响系统功能
            if (e.getMessage() != null && e.getMessage().contains("SQLITE_CONSTRAINT_PRIMARYKEY")) {
                log.info("normally exception!! tryDelTask():" + e.getMessage());
            }
        }
    }
}
