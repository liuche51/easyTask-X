package com.github.liuche51.easyTaskX.dao;
import com.github.liuche51.easyTaskX.dto.ScheduleSync;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleSyncDao {
    /**访问的db名称*/
    private static final String dbName= StringConstant.SCHEDULE_SYNC;
    /**可重入锁*/
    private static ReentrantLock lock=new ReentrantLock();
    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='schedule_sync';");
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

    public static void save(ScheduleSync scheduleSync) throws Exception {
        scheduleSync.setCreateTime(DateUtils.getCurrentDateTime());
        scheduleSync.setModifyTime(DateUtils.getCurrentDateTime());
        String sql = "insert into schedule_sync(transaction_id,schedule_id,slave,status,create_time,modify_time) values('"
                + scheduleSync.getTransactionId() + "','" + scheduleSync.getScheduleId() +  "','" + scheduleSync.getSlave() + "'," + scheduleSync.getStatus()
                + ",'" + scheduleSync.getCreateTime() + "','" + scheduleSync.getCreateTime() + "');";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static List<ScheduleSync> selectBySlaveAndStatusWithCount(String slave, short status, int count) throws SQLException, ClassNotFoundException {
        List<ScheduleSync> list = new ArrayList<>(count);
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_sync where slave='" + slave +
                    "' and status=" + status + " limit " + count + ";");
            while (resultSet.next()) {
                ScheduleSync scheduleSync = getScheduleSync(resultSet);
                list.add(scheduleSync);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleSyncDao->selectBySlaveAndStatusWithCount");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<ScheduleSync> selectByTaskId(String taskId) throws SQLException, ClassNotFoundException {
        List<ScheduleSync> list = new ArrayList<>(2);
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_sync where schedule_id='" + taskId + "';");
            while (resultSet.next()) {
                ScheduleSync scheduleSync = getScheduleSync(resultSet);
                list.add(scheduleSync);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleSyncDao->selectByTaskId");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    private static ScheduleSync getScheduleSync(ResultSet resultSet) throws SQLException {
        String transactionId = resultSet.getString("transaction_id");
        String scheduleId = resultSet.getString("schedule_id");
        String slave = resultSet.getString("slave");
        short status1 = resultSet.getShort("status");
        String createTime = resultSet.getString("create_time");
        String modifyTime = resultSet.getString("modify_time");
        ScheduleSync scheduleSync = new ScheduleSync();
        scheduleSync.setTransactionId(transactionId);
        scheduleSync.setScheduleId(scheduleId);
        scheduleSync.setSlave(slave);
        scheduleSync.setStatus(status1);
        scheduleSync.setCreateTime(createTime);
        scheduleSync.setModifyTime(modifyTime);
        return scheduleSync;
    }

    public static void updateSlaveAndStatusByFollow(String oldSlave, String newSlave, short status) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set slave='" + newSlave + "', status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where slave='" + oldSlave + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void updateStatusBySlaveAndStatus(String slave, short status, short updateStatus) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + updateStatus + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where slave='" + slave + "' and status=" + status + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void updateStatusByFollowAndScheduleIds(String slave, String[] scheduleIds, short updateStatus) throws SQLException, ClassNotFoundException {
        String str = SqliteHelper.getInConditionStr(scheduleIds);
        String sql = "update schedule_sync set status=" + updateStatus + ",modify_time='" + DateUtils.getCurrentDateTime()
                + "' where slave='" + slave + "' and schedule_id in" + str + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void updateStatusByScheduleIdAndSlave(String scheduleId, String slave, short status) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where schedule_id='" + scheduleId + "' and slave='" + slave + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateStatusAndTransactionIdByScheduleId(String scheduleId, short status,String transactionId) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + status + ",transaction_id='" + transactionId + "',modify_time='" + DateUtils.getCurrentDateTime() + "' where schedule_id='" + scheduleId + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateStatusByTransactionIds(String[] transactionIds,short status) throws SQLException, ClassNotFoundException {
        String instr=SqliteHelper.getInConditionStr(transactionIds);
        String sql = "update schedule_sync set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where transaction_id in " + instr + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteByTransactionIdAndSlave(String transactionId, String slave) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_sync where transaction_id='" + transactionId + "' and slave='" + slave + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteByStatus(short status) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_sync where status = " + status+";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_sync;";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
}
