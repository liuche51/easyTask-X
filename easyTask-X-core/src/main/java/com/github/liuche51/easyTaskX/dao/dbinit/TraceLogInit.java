package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.dao.LogErrorDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.sql.SQLException;

/**
 * 任务跟踪记录
 */
public class TraceLogInit {
    public static void initTraceLog() throws SQLException, ClassNotFoundException {
        boolean exist = LogErrorDao.existTable();
        if (!exist) {
            String sql = "CREATE TABLE \"" + DbTableName.TRACE_LOG + "\" (\n" +
                    "\"taskid\"  TEXT NOT NULL,\n" +
                    "\"content\"  TEXT,\n" +
                    "\"create_time\"  TEXT\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.TRACE_LOG);
            helper.executeUpdate(sql);
        }
    }
}
