package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DbInit {
    private static Logger log = LoggerFactory.getLogger(DbInit.class);
    public static boolean hasInit = false;//数据库是否已经初始化

    /**
     * 数据库初始化。需要避免多线程
     *
     * @return
     */
    public static synchronized boolean init() {
        if (hasInit)
            return true;
        try {
            //创建db存储文件夹
            File file = new File(NodeService.getConfig().getTaskStorePath());
            if (!file.exists()) {
                file.mkdirs();
            }
            ScheduleInit.initSchedule();
            ScheduleInit.initTranlog();
            ScheduleInit.initBinlog();
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
            boolean exist3 = ScheduleSyncDao.existTable();
            if (!exist3) {
                //本地待运行的任务
                String sql3 = "CREATE TABLE \"schedule_sync\" (\n" +
                        "\"transaction_id\"  TEXT,\n" +
                        "\"schedule_id\"  TEXT NOT NULL,\n" +
                        "\"slave\"  TEXT,\n" +
                        "\"status\"  INTEGER,\n" +
                        "\"create_time\"  TEXT,\n" +
                        "\"modify_time\"  TEXT\n" +
                        ");";
                SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE_SYNC);
                helper.executeUpdate(sql3);
                String indexsql = "CREATE INDEX index_scheduleId ON schedule_sync (schedule_id);";
                SqliteHelper helper2 = new SqliteHelper(DbTableName.SCHEDULE_SYNC);
                helper2.executeUpdate(indexsql);
            }
            hasInit = true;
            log.info("Sqlite DB 初始化完成");
            return true;
        } catch (Exception e) {
            log.error("easyTask db init fail.", e);
            return false;
        }
    }
}
