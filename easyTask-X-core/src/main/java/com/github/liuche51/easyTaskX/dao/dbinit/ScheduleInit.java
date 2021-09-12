package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dao.TranlogScheduleDao;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.sql.SQLException;

/**
 * 任务库相关表初始化
 */
public class ScheduleInit {
    public static void initSchedule() throws SQLException, ClassNotFoundException {
        boolean exist = ScheduleDao.existTable();
        if (!exist) {
            //本地待运行的任务
            String sql = "CREATE TABLE \"" + DbTableName.SCHEDULE + "\" (\n" +
                    "\"id\"  TEXT NOT NULL,\n" +
                    "\"class_path\"  TEXT,\n" +
                    "\"execute_time\"  INTEGER,\n" +
                    "\"task_type\"  TEXT,\n" +
                    "\"period\"  INTEGER,\n" +
                    "\"unit\"  TEXT,\n" +
                    "\"param\"  TEXT,\n" +
                    "\"transaction_id\"  TEXT,\n" +
                    "\"create_time\"  TEXT,\n" +
                    "\"modify_time\"  TEXT,\n" +
                    "\"source\"  TEXT,\n" +
                    "\"executer\"  TEXT,\n" +
                    "PRIMARY KEY (\"id\" ASC)\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE);
            helper.executeUpdate(sql);
            String indexsq = "CREATE UNIQUE INDEX index_transactionId ON " + DbTableName.SCHEDULE + " (transaction_id);CREATE INDEX index_executer ON schedule (executer);";
            SqliteHelper helper2 = new SqliteHelper(DbTableName.SCHEDULE);
            helper2.executeUpdate(indexsq);
        }
    }

    public static void initTranlog() throws SQLException, ClassNotFoundException {
        boolean exist4 = TranlogScheduleDao.existTable();
        if (!exist4) {
            //本地待运行的任务
            String sql4 = "CREATE TABLE \"" + DbTableName.TRANLOG_SCHEDULE + "\" (\n" +
                    "\"id\"  TEXT NOT NULL,\n" +
                    "\"content\"  TEXT,\n" +
                    "\"type\"  INTEGER,\n" +
                    "\"status\"  INTEGER,\n" +
                    "\"slaves\"  TEXT,\n" +
                    "\"retry_time\"  TEXT,\n" +
                    "\"retry_count\"  INTEGER,\n" +
                    "\"create_time\"  TEXT,\n" +
                    "\"modify_time\"  TEXT,\n" +
                    "PRIMARY KEY (\"id\" ASC)\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE);
            helper.executeUpdate(sql4);
            String indexsql = "CREATE INDEX index_status_type ON " + DbTableName.TRANLOG_SCHEDULE + " (status,type);";
            SqliteHelper helper2 = new SqliteHelper(DbTableName.SCHEDULE);
            helper2.executeUpdate(indexsql);
        }
    }

    public static void initBinlog() throws SQLException, ClassNotFoundException {
        boolean exist = BinlogScheduleDao.existTable();
        if (!exist) {
            //本地待运行的任务
            String sql = "CREATE TABLE \"" + DbTableName.BINLOG_SCHEDULE + "\" (\n" +
                    "\"id\"  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\n" +
                    "\"sql\"  TEXT,\n" +
                    "\"create_time\"  TEXT\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE);
            helper.executeUpdate(sql);
        }
    }
}
