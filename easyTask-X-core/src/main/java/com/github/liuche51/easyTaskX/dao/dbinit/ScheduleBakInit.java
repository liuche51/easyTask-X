package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.sql.SQLException;

/**
 * 任务备份库相关表初始化
 */
public class ScheduleBakInit {
    public static void initSchedule() throws SQLException, ClassNotFoundException {
        boolean exist2 = ScheduleBakDao.existTable();
        if (!exist2) {
            //备份其他节点的任务
            String sql2 = "CREATE TABLE \"" + DbTableName.SCHEDULE_BAK + "\" (\n" +
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
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE_BAK);
            helper.executeUpdate(sql2);
            String indexsql = "CREATE UNIQUE INDEX index_transactionId ON schedule_bak (transaction_id);";
            SqliteHelper helper2 = new SqliteHelper(DbTableName.SCHEDULE_BAK);
            helper2.executeUpdate(indexsql);
        }
    }

    public static void initTranlog() throws SQLException, ClassNotFoundException {
        boolean exist4 = TranlogScheduleDao.existTable();
        if (!exist4) {
            //本地待运行的任务
            String sql4 = "CREATE TABLE \"" + DbTableName.TRANLOG_SCHEDULE_BAK + "\" (\n" +
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
            SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE_BAK);
            helper.executeUpdate(sql4);
            String indexsql = "CREATE INDEX index_status_type ON " + DbTableName.TRANLOG_SCHEDULE_BAK + " (status,type);";
            SqliteHelper helper2 = new SqliteHelper(DbTableName.SCHEDULE_BAK);
            helper2.executeUpdate(indexsql);
        }
    }
}
