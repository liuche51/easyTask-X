package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.TraceLog;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.LogUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class TraceLogDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.TRACE_LOG;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.TRACE_LOG;
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

    public static void saveBatch(List<TraceLog> traceLogs) {
        try {
            if (!DbInit.hasInit)
                DbInit.init();
            traceLogs.forEach(x -> {
                x.setCreateTime(DateUtils.getCurrentDateTime());
            });
            String sql = contactSaveSql(traceLogs);
            SqliteHelper.executeUpdateForSync(sql, dbName, lock);
        } catch (Exception e) {
            LogUtil.error("", e);
        }
    }

    private static String contactSaveSql(List<TraceLog> traceLogs) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(taskid,content,create_time) values");
        for (TraceLog log : traceLogs) {
            sql1.append("('");
            sql1.append(log.getTaskid()).append("','");
            sql1.append(log.getContent()).append("','");
            sql1.append(log.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
