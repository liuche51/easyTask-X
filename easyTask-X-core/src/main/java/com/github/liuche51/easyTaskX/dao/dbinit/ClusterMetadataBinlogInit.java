package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.dao.BinlogClusterMetaDao;
import com.github.liuche51.easyTaskX.dao.LogErrorDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.sql.SQLException;

public class ClusterMetadataBinlogInit {
    public static void initClusterMetadataBinlog() throws SQLException, ClassNotFoundException {
        boolean exist = BinlogClusterMetaDao.existTable();
        if (!exist) {
            //本地待运行的任务
            String sql = "CREATE TABLE \"" + DbTableName.LOG_CLUSTERMETA + "\" (\n" +
                    "\"id\"  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\n" +
                    "\"optype\"  TEXT,\n" +
                    "\"regnode\"  TEXT,\n" +
                    "\"key\"  TEXT,\n" +
                    "\"value\"  TEXT,\n" +
                    "\"create_time\"  TEXT\n" +
                    ");";
            SqliteHelper helper = new SqliteHelper(DbTableName.LOG_CLUSTERMETA);
            helper.executeUpdate(sql);
        }
    }
}
