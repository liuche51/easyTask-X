package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.LogError;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class LogErrorDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.LOG_ERROR;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.LOG_ERROR;
    /**
     * 可重入锁
     */
    private static ReentrantLock lock = new ReentrantLock();
    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='" + tableName + "';");
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0)
                    return true;
            }
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static void saveBatch(List<LogError> logErrors) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        logErrors.forEach(x -> {
            x.setCreateTime(DateUtils.getCurrentDateTime());
        });
        String sql = contactSaveSql(logErrors);
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    private static String contactSaveSql(List<LogError> logErrors) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,content,detail,type,status,create_time) values");
        for (LogError log : logErrors) {
            sql1.append("('");
            sql1.append(log.getId()).append("','");
            sql1.append(log.getContent()).append("','");
            sql1.append(log.getDetail()).append(",'");
            sql1.append(log.getType()).append(",'");
            sql1.append(log.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
