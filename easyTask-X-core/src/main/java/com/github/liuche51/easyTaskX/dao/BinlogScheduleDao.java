package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
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

public class BinlogScheduleDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.SCHEDULE;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.BINLOG_SCHEDULE;

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

    public static void save(String schedulesql,String scheduleId,int status, SqliteHelper helper) throws SQLException {
        BinlogSchedule binlogSchedule = new BinlogSchedule();
        binlogSchedule.setSql(schedulesql);
        binlogSchedule.setScheduleId(scheduleId);
        binlogSchedule.setStatus(status);
        String sql = contactSaveSql(Arrays.asList(binlogSchedule));
        helper.executeUpdate(sql);
    }
    public static void saveBatch(List<BinlogSchedule> schedules, SqliteHelper helper) throws SQLException {
        String sql = contactSaveSql(schedules);
        helper.executeUpdate(sql);
    }

    public static List<BinlogSchedule> getScheduleBinlogByIndex(long index, int count) throws SQLException {
        List<BinlogSchedule> list = new LinkedList<>();
        String sql = "SELECT * FROM " + tableName + " where id > " + index + " limit " + count + ";";
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery(sql);
            while (resultSet.next()) {
                BinlogSchedule binlogSchedule = getBinlogSchedule(resultSet);
                list.add(binlogSchedule);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleDao->getScheduleBinlogByIndex");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    private static BinlogSchedule getBinlogSchedule(ResultSet resultSet) throws SQLException {
        long id = resultSet.getLong("id");
        String sql = resultSet.getString("sql");
        String scheduleId = resultSet.getString("schedule_id");
        int status = resultSet.getInt("status");
        String createTime = resultSet.getString("create_time");
        BinlogSchedule binlogSchedule = new BinlogSchedule();
        binlogSchedule.setId(id);
        binlogSchedule.setSql(sql);
        binlogSchedule.setScheduleId(scheduleId);
        binlogSchedule.setStatus(status);
        binlogSchedule.setCreateTime(createTime);
        return binlogSchedule;
    }

    private static String contactSaveSql(List<BinlogSchedule> binlogSchedules) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,sql,schedule_id,status,create_time) values");
        for (BinlogSchedule binlogSchedule : binlogSchedules) {
            binlogSchedule.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(binlogSchedule.getId()).append("','");
            sql1.append(binlogSchedule.getSql()).append("','");
            sql1.append(binlogSchedule.getScheduleId()).append("',");
            sql1.append(binlogSchedule.getStatus()).append(",'");
            sql1.append(binlogSchedule.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
