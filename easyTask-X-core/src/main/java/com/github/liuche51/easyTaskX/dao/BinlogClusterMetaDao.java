package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BinlogClusterMetaDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.LOG_CLUSTERMETA;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.LOG_CLUSTERMETA;
    /**
     * 可重入锁。避免Schedule库多线程写操作。同一个库不同表也不能同时写。
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

    public static void saveBatch(List<BinlogClusterMeta> binlogClusterMetas) throws SQLException, ClassNotFoundException {
        if (!DbInit.hasInit)
            DbInit.init();
        String sql = contactSaveSql(binlogClusterMetas);
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static List<BinlogClusterMeta> getBinlogClusterMetaByIndex(long index, int count) throws SQLException {
        List<BinlogClusterMeta> list = new LinkedList<>();
        String sql = "SELECT * FROM " + tableName + " where id > " + index + " limit " + count + ";";
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery(sql);
            while (resultSet.next()) {
                BinlogClusterMeta binlogSchedule = getBinlogClusterMeta(resultSet);
                list.add(binlogSchedule);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleDao->getScheduleBinlogByIndex");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    private static BinlogClusterMeta getBinlogClusterMeta(ResultSet resultSet) throws SQLException {
        long id = resultSet.getLong("id");
        String optype = resultSet.getString("optype");
        String regnode = resultSet.getString("regnode");
        String value = resultSet.getString("value");
        String key = resultSet.getString("key");
        String createTime = resultSet.getString("create_time");
        BinlogClusterMeta binlogClusterMeta = new BinlogClusterMeta();
        binlogClusterMeta.setId(id);
        binlogClusterMeta.setOptype(optype);
        binlogClusterMeta.setOptype(regnode);
        binlogClusterMeta.setOptype(value);
        binlogClusterMeta.setOptype(key);
        binlogClusterMeta.setCreateTime(createTime);
        return binlogClusterMeta;
    }

    private static String contactSaveSql(List<BinlogClusterMeta> binlogClusterMetas) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,optype,regnode,key,value,create_time) values");
        for (BinlogClusterMeta binlogClusterMeta : binlogClusterMetas) {
            binlogClusterMeta.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(binlogClusterMeta.getId()).append("','");
            sql1.append(binlogClusterMeta.getOptype()).append("',");
            sql1.append(binlogClusterMeta.getRegnode()).append("',");
            sql1.append(binlogClusterMeta.getKey()).append("',");
            sql1.append(binlogClusterMeta.getValue()).append("',");
            sql1.append(binlogClusterMeta.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
