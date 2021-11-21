package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.HistorySchedule;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class HistoryScheduleDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.HIS_SCHEDULE;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.HIS_SCHEDULE;
    /**
     * 可重入锁
     */
    private static ReentrantLock lock = new ReentrantLock();

    public static boolean existTable() throws SQLException {
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

    public static void saveBatch(List<HistorySchedule> historySchedules) throws Exception {
        String sql = contactSaveSql(historySchedules);
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    private static HistorySchedule getHistorySchedule(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String content = resultSet.getString("content");
        String createTime = resultSet.getString("create_time");
        HistorySchedule historySchedule = new HistorySchedule();
        historySchedule.setId(id);
        historySchedule.setContent(content);
        historySchedule.setCreateTime(createTime);
        return historySchedule;
    }

    private static String contactSaveSql(List<HistorySchedule> historySchedules) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,content,create_time) values");
        for (HistorySchedule historySchedule : historySchedules) {
            historySchedule.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(historySchedule.getId()).append("','");
            sql1.append(historySchedule.getContent()).append("','");
            sql1.append(historySchedule.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
