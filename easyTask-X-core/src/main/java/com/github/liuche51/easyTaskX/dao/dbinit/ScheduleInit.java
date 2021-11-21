package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
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
                    "\"create_time\"  TEXT,\n" +
                    "\"modify_time\"  TEXT,\n" +
                    "\"source\"  TEXT,\n" +
                    "\"executer\"  TEXT,\n" +
                    "\"status\"  INTEGER,\n" +
                    "PRIMARY KEY (\"id\" ASC)\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE);
            helper.executeUpdate(sql);
            String indexsq = "CREATE UNIQUE INDEX index_transactionId ON " + DbTableName.SCHEDULE + " (transaction_id);CREATE INDEX index_executer ON schedule (executer);";
            SqliteHelper helper2 = new SqliteHelper(DbTableName.SCHEDULE);
            helper2.executeUpdate(indexsq);
        }
    }

    public static void initBinlog() throws SQLException, ClassNotFoundException {
        boolean exist = BinlogScheduleDao.existTable();
        if (!exist) {
            //本地待运行的任务
            String sql = "CREATE TABLE \"" + DbTableName.BINLOG_SCHEDULE + "\" (\n" +
                    "\"id\"  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\n" +
                    "\"sql\"  TEXT,\n" +
                    "\"schedule_id\"  TEXT,\n" +
                    "\"status\"  INTEGER,\n" +
                    "\"create_time\"  TEXT\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE);
            helper.executeUpdate(sql);
        }
    }
    public static void initHistory() throws SQLException, ClassNotFoundException {
        boolean exist = BinlogScheduleDao.existTable();
        if (!exist) {
            //本地待运行的任务
            String sql = "CREATE TABLE \"" + DbTableName.HIS_SCHEDULE + "\" (\n" +
                    "\"id\"  TEXT PRIMARY KEY NOT NULL,\n" +
                    "\"content\"  TEXT,\n" +
                    "\"create_time\"  TEXT\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.HIS_SCHEDULE);
            helper.executeUpdate(sql);
        }
    }
}
