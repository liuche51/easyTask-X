package com.github.liuche51.easyTaskX.util;

import com.github.liuche51.easyTaskX.dao.LogErrorDao;
import com.github.liuche51.easyTaskX.dto.db.LogError;
import com.github.liuche51.easyTaskX.enume.LogErrorTypeEnum;

import java.util.Arrays;

/**
 * 各种系统重大异常记录
 */
public class LogErrorUtil {
    public static void writeRpcErrorMsgToDb(String content, String detail) {
        LogError logError = new LogError(content, detail, LogErrorTypeEnum.RPC);
        LogErrorDao.saveBatch(Arrays.asList(logError));
    }

    public static void writeQueueErrorMsgToDb(String content, String detail) {
        LogError logError = new LogError(content, detail, LogErrorTypeEnum.QUEUE);
        LogErrorDao.saveBatch(Arrays.asList(logError));
    }
}
