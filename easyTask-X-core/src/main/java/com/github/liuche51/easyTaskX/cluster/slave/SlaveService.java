package com.github.liuche51.easyTaskX.cluster.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.TransactionStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;

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
        transactionLog.setType(TransactionTypeEnum.SAVE);
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
     * 取消备份任务
     *
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void cancelSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TranlogScheduleBakDao.updateStatusById(transactionId, TransactionStatusEnum.CANCEL);
    }
}
