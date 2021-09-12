package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BinlogScheduleDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.SCHEDULE;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.BINLOG_SCHEDULE;
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

    public static void save(BinlogSchedule binlogSchedule) throws SQLException, ClassNotFoundException {
        if (!DbInit.hasInit)
            DbInit.init();
        String sql = contactSaveSql(Arrays.asList(binlogSchedule));
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static void saveBatch(List<BinlogSchedule> binlogSchedules) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        String sql = contactSaveSql(binlogSchedules);
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    private static BinlogSchedule getBinlogSchedule(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String sql = resultSet.getString("sql");
        String createTime = resultSet.getString("create_time");
        BinlogSchedule binlogSchedule = new BinlogSchedule();
        binlogSchedule.setId(id);
        binlogSchedule.setSql(sql);
        binlogSchedule.setCreateTime(createTime);
        return binlogSchedule;
    }

    private static String contactSaveSql(List<BinlogSchedule> binlogSchedules) {
        StringBuilder sql1 = new StringBuilder("insert into "+tableName+"(id,sql,create_time) values");
        for (BinlogSchedule binlogSchedule : binlogSchedules) {
            binlogSchedule.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(binlogSchedule.getId()).append("','");
            sql1.append(binlogSchedule.getSql()).append("',");
            sql1.append(binlogSchedule.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
