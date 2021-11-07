package com.github.liuche51.easyTaskX.dao;


import com.github.liuche51.easyTaskX.dao.dbinit.DbInit;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.enume.ScheduleStatusEnum;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.StringConstant;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleDao {
    /**
     * 访问的db名称
     */
    private static final String dbName = DbTableName.SCHEDULE;
    /**
     * 访问的表名称
     */
    private static final String tableName = DbTableName.SCHEDULE;
    /**
     * 可重入锁。避免Schedule库多线程写操作。同一个库不同表也不能同时写。
     */
    private static ReentrantLock lock = new ReentrantLock();

    public static ReentrantLock getLock() {
        return lock;
    }

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

    public static void saveBatch(List<Schedule> schedules) throws SQLException {
        SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, ScheduleDao.getLock());
        helper.beginTran();
        try {
            String sql = contactSaveSql(schedules);
            helper.executeUpdate(sql);
            List<BinlogSchedule> binlogSchedules = new ArrayList<>(schedules.size());
            schedules.forEach(x -> {
                BinlogSchedule binlogSchedule = new BinlogSchedule();
                binlogSchedule.setSql(contactSaveSql(Arrays.asList(x)));
                binlogSchedule.setScheduleId(x.getId());
                binlogSchedule.setStatus(x.getStatus());
                binlogSchedules.add(binlogSchedule);
            });
            BinlogScheduleDao.saveBatch(binlogSchedules, helper);
            helper.commitTran();
        } finally {
            helper.destroyed();
        }
    }

    public static List<Schedule> selectByIds(String[] ids) throws SQLException {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            String instr = SqliteHelper.getInConditionStr(ids);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where id in " + instr + ";");
            while (resultSet.next()) {
                Schedule schedule = getSchedule(resultSet);
                list.add(schedule);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleDao->selectByIds");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<Schedule> selectByTaskId(String taskId) throws SQLException {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where id = '" + taskId + "';");
            while (resultSet.next()) {
                Schedule schedule = getSchedule(resultSet);
                list.add(schedule);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleDao->selectByTaskId");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static boolean isExistByExecuter(String executer) throws SQLException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT count(0) as count FROM " + tableName + " where executer ='" + executer + "';");
            while (resultSet.next()) {
                int count = resultSet.getInt("count");
                if (count > 0) return true;
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleDao->isExistByExecuter");
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static List<Schedule> selectByExecuter(String executer, int count) throws SQLException {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM " + tableName + " where executer = '" + executer + " limit " + count + ";");
            while (resultSet.next()) {
                Schedule schedule = getSchedule(resultSet);
                list.add(schedule);
            }
        } catch (SQLiteException e) {
            SqliteHelper.writeDatabaseLockedExceptionLog(e, "ScheduleDao->selectIdsByExecuter");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    /**
     * 通用更新
     *
     * @param ids
     * @param values
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void updateByIds(String[] ids, Map<String, Object> values) throws SQLException {
        Iterator<Map.Entry<String, Object>> items2 = values.entrySet().iterator();
        //组装更新字段。
        StringBuilder updatestr = new StringBuilder();
        while (items2.hasNext()) {
            Map.Entry<String, Object> item2 = items2.next();
            switch (item2.getKey()) {
                case "executer":
                    updatestr.append("executer='").append(item2.getValue()).append("'");
                    break;
                case "status":
                    updatestr.append("status=").append(item2.getValue());
                    break;
                default:
                    break;
            }
        }
        String instr = SqliteHelper.getInConditionStr(ids);
        String sql = "UPDATE " + tableName + " set " + updatestr.toString() + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where id in " + instr + ";";
        SqliteHelper helper = new SqliteHelper(DbTableName.SCHEDULE, lock);
        try {
            helper.beginTran();
            helper.executeUpdate(sql);
            BinlogScheduleDao.save(sql, StringConstant.EMPTY, ScheduleStatusEnum.NORMAL, helper);
            helper.commitTran();
        } finally {
            helper.destroyed();
        }
    }

    public static void deleteByIds(String[] ids, SqliteHelper helper) throws SQLException {
        String instr = SqliteHelper.getInConditionStr(ids);
        String sql = "delete FROM " + tableName + " where id in" + instr + ";";
        helper.executeUpdate(sql);
        BinlogScheduleDao.save(sql, StringConstant.EMPTY, ScheduleStatusEnum.NORMAL, helper);
    }

    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM " + tableName + ";";
        SqliteHelper.executeUpdateForSync(sql, dbName, lock);
    }

    private static Schedule getSchedule(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String classPath = resultSet.getString("class_path");
        Long executeTime = resultSet.getLong("execute_time");
        String taskType = resultSet.getString("task_type");
        Long period = resultSet.getLong("period");
        String unit = resultSet.getString("unit");
        String param = resultSet.getString("param");
        String createTime = resultSet.getString("create_time");
        String modifyTime = resultSet.getString("modify_time");
        String source = resultSet.getString("source");
        String executer = resultSet.getString("executer");
        int status = resultSet.getInt("status");

        Schedule schedule = new Schedule();
        schedule.setId(id);
        schedule.setClassPath(classPath);
        schedule.setExecuteTime(executeTime);
        schedule.setTaskType(taskType);
        schedule.setPeriod(period);
        schedule.setUnit(unit);
        schedule.setParam(param);
        schedule.setCreateTime(createTime);
        schedule.setModifyTime(modifyTime);
        schedule.setSource(source);
        schedule.setExecuter(executer);
        schedule.setStatus(status);
        return schedule;
    }

    private static String contactSaveSql(List<Schedule> schedules) {
        StringBuilder sql1 = new StringBuilder("insert into " + tableName + "(id,class_path,execute_time,task_type,period,unit,param,create_time,modify_time,executer,status,source) values");
        for (Schedule schedule : schedules) {
            schedule.setCreateTime(DateUtils.getCurrentDateTime());
            schedule.setModifyTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(schedule.getId()).append("','");
            sql1.append(schedule.getClassPath()).append("',");
            sql1.append(schedule.getExecuteTime()).append(",'");
            sql1.append(schedule.getTaskType()).append("',");
            sql1.append(schedule.getPeriod()).append(",'");
            sql1.append(schedule.getUnit()).append("','");
            sql1.append(schedule.getParam()).append("','");
            sql1.append(schedule.getCreateTime()).append("','");
            sql1.append(schedule.getModifyTime()).append("','");
            sql1.append(schedule.getExecuter()).append("',");
            sql1.append(schedule.getStatus()).append(",'");
            sql1.append(schedule.getSource()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
