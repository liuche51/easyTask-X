package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.LogErrorDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.sql.SQLException;

/**
 * 系统致命错误记录
 */
public class LogInit {
    public static void initlLogError() throws SQLException, ClassNotFoundException {
        boolean exist = LogErrorDao.existTable();
        if (!exist) {
            //本地待运行的任务
            String sql = "CREATE TABLE \"" + DbTableName.LOG_ERROR + "\" (\n" +
                    "\"id\"  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\n" +
                    "\"content\"  TEXT,\n" +
                    "\"detail\"  TEXT,\n" +
                    "\"type\"  TEXT,\n" +
                    "\"create_time\"  TEXT\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE);
            helper.executeUpdate(sql);
        }
    }
}
