package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.TranlogSchedule;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class TranlogScheduleDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.SCHEDULE;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.TRANLOG_SCHEDULE;
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

    public static void saveBatch(List<TranlogSchedule> transactionLogs) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        transactionLogs.forEach(x -> {
            x.setRetryCount(new Short("0"));
            x.setRetryTime(StringConstant.EMPTY);
            x.setCreateTime(DateUtils.getCurrentDateTime());
            x.setModifyTime(DateUtils.getCurrentDateTime());
        });
        String sql = contactSaveSql(transactionLogs);
        SqliteHelper.executeUpdateForSync(sql, dbName, ScheduleDao.getLock());
    }

    public static void updateStatusById(String id, short status) throws SQLException, ClassNotFoundException {
        String sql = "update " + tableName + " set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where id='" + id + "';";
        SqliteHelper.executeUpdateForSync(sql, dbName, ScheduleDao.getLock());
    }
    public static void updateStatusById(String id, short status,SqliteHelper helper) throws SQLException, ClassNotFoundException {
        String sql = "update " + tableName + " set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where id='" + id + "';";
        SqliteHelper.executeUpdateForSync(sql, dbName, ScheduleDao.getLock());
    }

    public static void updateStatusByIds(String[] ids, short status,SqliteHelper helper ) throws SQLException {
        String str = SqliteHelper.getInConditionStr(ids);
        String sql = "update " + tableName + " set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where id in" + str + ";";
        helper.executeUpdate(sql);
    }

    public static List<TranlogSchedule> selectByStatusAndType(short status,int count) throws SQLException, ClassNotFoundException {
        List<TranlogSchedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where status = " + status + " limit " + count + ";");
            while (resultSet.next()) {
                TranlogSchedule transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "TranlogScheduleDao->selectByStatusAndType");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<TranlogSchedule> selectByStatusAndType(short[] status, int count) throws SQLException, ClassNotFoundException {
        List<TranlogSchedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            String instr = SqliteHelper.getInConditionStr(status);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where status in " + instr +" limit " + count + ";");
            while (resultSet.next()) {
                TranlogSchedule transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "TransactionLogDao->selectByStatusAndType");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<TranlogSchedule> selectByStatusAndReTryCount(short status,  short lessThanCancelReTryCount, int count) throws SQLException, ClassNotFoundException {
        List<TranlogSchedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where status = " + status + " and retry_count<" + lessThanCancelReTryCount + " limit " + count + ";");
            while (resultSet.next()) {
                TranlogSchedule transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "TransactionLogDao->selectByStatusAndReTryCount");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<TranlogSchedule> selectByStatus(short status) throws SQLException {
        List<TranlogSchedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where status = " + status + ";");
            while (resultSet.next()) {
                TranlogSchedule transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "TransactionLogDao->selectByStatus");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<TranlogSchedule> selectByTaskId(String taskId) throws SQLException {
        List<TranlogSchedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where content like '%" + taskId + "%';");
            while (resultSet.next()) {
                TranlogSchedule transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "TransactionLogDao->selectByTaskId");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static void updateRetryInfoById(String id, short retryCount, String retryTime) throws SQLException, ClassNotFoundException {
        String sql = "update " + tableName + " set retry_count=" + retryCount + ",modify_time='" + retryTime + "',modify_time='" + DateUtils.getCurrentDateTime() + "' where id='" + id + "';";
        SqliteHelper.executeUpdateForSync(sql, dbName, ScheduleDao.getLock());
    }
    public static void deleteByStatus(short status) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM " + tableName + " where status = " + status + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, ScheduleDao.getLock());
    }

    public static void deleteByIds(String[] ids) throws SQLException, ClassNotFoundException {
        if (ids == null || ids.length == 0) return;
        String instr = SqliteHelper.getInConditionStr(ids);
        String sql = "delete FROM " + tableName + " where id in " + instr + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, ScheduleDao.getLock());
    }
    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM " + tableName + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }
    private static TranlogSchedule getTransaction(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String content = resultSet.getString("content");
        short status = resultSet.getShort("status");
        String retryTime = resultSet.getString("retry_time");
        short retryCount = resultSet.getShort("retry_count");
        String slaves = resultSet.getString("slaves");
        String modifyTime = resultSet.getString("modify_time");
        String createTime = resultSet.getString("create_time");
        TranlogSchedule transactionLog = new TranlogSchedule();
        transactionLog.setId(id);
        transactionLog.setStatus(status);
        transactionLog.setContent(content);
        transactionLog.setRetryTime(retryTime);
        transactionLog.setRetryCount(retryCount);
        transactionLog.setSlaves(slaves);
        transactionLog.setModifyTime(modifyTime);
        transactionLog.setCreateTime(createTime);
        return transactionLog;
    }

    private static String contactSaveSql(List<TranlogSchedule> transactionLogs) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,content,type,status,follows,retry_time,retry_count,create_time,modify_time) values");
        for (TranlogSchedule log : transactionLogs) {
            sql1.append("('");
            sql1.append(log.getId()).append("','");
            sql1.append(log.getContent()).append("','");
            sql1.append(log.getStatus()).append(",'");
            sql1.append(log.getRetryTime()).append("',");
            sql1.append(log.getRetryCount()).append(",'");
            sql1.append(log.getSlaves()).append("','");
            sql1.append(log.getCreateTime()).append("','");
            sql1.append(log.getModifyTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
