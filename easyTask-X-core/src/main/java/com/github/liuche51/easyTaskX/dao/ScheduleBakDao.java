package com.github.liuche51.easyTaskX.dao;

import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleBakDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.SCHEDULE_BAK;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.SCHEDULE_BAK;
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

    public static void executeSql(String sql) throws SQLException, ClassNotFoundException {
        SqliteHelper.executeUpdateForSync(sql, tableName, lock);
    }

    public static void save(ScheduleBak scheduleBak) throws SQLException, ClassNotFoundException {
        if (!DbInit.hasInit)
            DbInit.init();
        scheduleBak.setCreateTime(DateUtils.getCurrentDateTime());
        scheduleBak.setModifyTime(DateUtils.getCurrentDateTime());
        String sql = contactSaveSql(Arrays.asList(scheduleBak));
        SqliteHelper.executeUpdateForSync(sql, tableName, lock);
    }

    public static void saveBatch(List<ScheduleBak> scheduleBaks) throws Exception {
        scheduleBaks.forEach(x -> {
            x.setCreateTime(DateUtils.getCurrentDateTime());
            x.setModifyTime(DateUtils.getCurrentDateTime());
        });
        String sql = contactSaveSql(scheduleBaks);
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static void delete(String id) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM " + tableName + " where id='" + id + "';";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static void deleteByIds(String[] ids) throws SQLException, ClassNotFoundException {
        String instr = SqliteHelper.getInConditionStr(ids);
        String sql = "delete FROM " + tableName + " where id in" + instr + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM " + tableName + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static void deleteBySource(String source) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM " + tableName + " where source='" + source + "';";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static void deleteNotInBySources(String[] sources) throws SQLException, ClassNotFoundException {
        if (sources == null || sources.length == 0) return;
        String conditionStr = SqliteHelper.getInConditionStr(sources);
        String sql = "delete FROM " + tableName + " where source not in" + conditionStr + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    public static List<ScheduleBak> getBySourceWithCount(String source, int count) throws SQLException, ClassNotFoundException {
        List<ScheduleBak> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where source='" + source + "' limit " + count + ";");
            while (resultSet.next()) {
                ScheduleBak schedulebak = getScheduleBak(resultSet);
                list.add(schedulebak);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleBakDao->getBySourceWithCount");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<ScheduleBak> selectByTaskId(String taskId) throws SQLException, ClassNotFoundException {
        List<ScheduleBak> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where id='" + taskId + "';");
            while (resultSet.next()) {
                ScheduleBak schedulebak = getScheduleBak(resultSet);
                list.add(schedulebak);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleBakDao->selectByTaskId");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    private static ScheduleBak getScheduleBak(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String classPath = resultSet.getString("class_path");
        Long executeTime = resultSet.getLong("execute_time");
        String taskType = resultSet.getString("task_type");
        Long period = resultSet.getLong("period");
        String unit = resultSet.getString("unit");
        String param = resultSet.getString("param");
        String source = resultSet.getString("source");
        String executer = resultSet.getString("executer");
        int status = resultSet.getInt("status");
        String createTime = resultSet.getString("create_time");
        ScheduleBak schedulebak = new ScheduleBak();
        schedulebak.setId(id);
        schedulebak.setClassPath(classPath);
        schedulebak.setExecuteTime(executeTime);
        schedulebak.setTaskType(taskType);
        schedulebak.setPeriod(period);
        schedulebak.setUnit(unit);
        schedulebak.setParam(param);
        schedulebak.setSource(source);
        schedulebak.setStatus(status);
        schedulebak.setCreateTime(createTime);
        return schedulebak;
    }

    private static String contactSaveSql(List<ScheduleBak> scheduleBaks) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,class_path,execute_time,task_type,period,unit,param,source,executer,status,create_time,modify_time) values");
        for (ScheduleBak scheduleBak : scheduleBaks) {
            scheduleBak.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(scheduleBak.getId()).append("','");
            sql1.append(scheduleBak.getClassPath()).append("',");
            sql1.append(scheduleBak.getExecuteTime()).append(",'");
            sql1.append(scheduleBak.getTaskType()).append("',");
            sql1.append(scheduleBak.getPeriod()).append(",'");
            sql1.append(scheduleBak.getUnit()).append("','");
            sql1.append(scheduleBak.getParam()).append("','");
            sql1.append(scheduleBak.getSource()).append("','");
            sql1.append(scheduleBak.getStatus()).append(",'");
            sql1.append(scheduleBak.getCreateTime()).append("','");
            sql1.append(scheduleBak.getModifyTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
