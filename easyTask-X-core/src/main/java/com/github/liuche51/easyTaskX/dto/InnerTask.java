package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.ImmediatelyTypeEnum;
import com.github.liuche51.easyTaskX.enume.TaskType;
import com.github.liuche51.easyTaskX.enume.TimeUnit;
import com.github.liuche51.easyTaskX.util.DateUtils;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * 内部使用的Task
 */
public class InnerTask {
    /**
     * 任务截止运行时间
     */
    private long executeTime;
    private TaskType taskType = TaskType.ONECE;
    private long period;
    private TimeUnit unit;
    private ImmediatelyTypeEnum immediatelyType = ImmediatelyTypeEnum.NONE;//立即执行类型
    private String id;
    private String taskClassPath;
    private String group = "Default";//默认分组
    private String source;
    private String broker;//任务所属
    private Map<String, String> param;
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public ImmediatelyTypeEnum getImmediatelyType() {
        return immediatelyType;
    }

    public void setImmediatelyType(ImmediatelyTypeEnum immediatelyType) {
        this.immediatelyType = immediatelyType;
    }

    public Map<String, String> getParam() {
        return param;
    }

    public void setParam(Map<String, String> param) {
        this.param = param;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTaskClassPath() {
        return taskClassPath;
    }

    public void setTaskClassPath(String taskClassPath) {
        this.taskClassPath = taskClassPath;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(ZonedDateTime startTime) {
        this.startTime = startTime;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(ZonedDateTime endTime) {
        this.endTime = endTime;
    }

    /**
     * 获取周期性任务下次执行时间。已当前时间为基准计算下次而不是上次截止执行时间
     *
     * @param period
     * @param unit
     * @return
     * @throws Exception
     */
    public static long getNextExcuteTimeStamp(long period, TimeUnit unit) throws Exception {
        switch (unit) {
            case DAYS:
                return ZonedDateTime.now().plusDays(period).toInstant().toEpochMilli();
            case HOURS:
                return ZonedDateTime.now().plusHours(period).toInstant().toEpochMilli();
            case MINUTES:
                return ZonedDateTime.now().plusMinutes(period).toInstant().toEpochMilli();
            case SECONDS:
                return ZonedDateTime.now().plusSeconds(period).toInstant().toEpochMilli();
            default:
                throw new Exception("unSupport TimeUnit type");
        }
    }

    private static TimeUnit getTimeUnit(String unit) {
        switch (unit) {
            case "DAYS":
                return TimeUnit.DAYS;
            case "HOURS":
                return TimeUnit.HOURS;
            case "MINUTES":
                return TimeUnit.MINUTES;
            case "SECONDS":
                return TimeUnit.SECONDS;
            default:
                return null;
        }
    }
    /**
     * 转换为protocol buffer对象
     *
     * @return
     */
    public ScheduleDto.Schedule toScheduleDto() throws Exception {
        ScheduleDto.Schedule.Builder builder = ScheduleDto.Schedule.newBuilder();
        builder.setId(this.getId()).setClassPath(this.getTaskClassPath()).setExecuteTime(this.getExecuteTime())
                .setTaskType(this.getTaskType().name()).setImmediatelyType(this.getImmediatelyType().name())
                .setPeriod(this.period).setUnit(this.getUnit().name()).setStartTime(DateUtils.getTimeStamp(this.startTime))
                .setEndTime(DateUtils.getTimeStamp(this.endTime)).setParam(JSONObject.toJSONString(this.getParam()))
                .setSource(BrokerService.getConfig().getAddress());
        return builder.build();
    }
    /**
     * 从protocol buffer对象转化为Task对象
     *
     * @param schedule
     * @return
     */
    public static InnerTask parseFromScheduleDto(ScheduleDto.Schedule schedule) throws Exception {
        InnerTask task = new InnerTask();
        task.setId(schedule.getId());
        task.setExecuteTime(schedule.getExecuteTime());
        task.setParam(JSONObject.parseObject(schedule.getParam(), Map.class));
        task.setPeriod(schedule.getPeriod());
        task.setTaskType(TaskType.getByValue(schedule.getTaskType()));
        task.setImmediatelyType(ImmediatelyTypeEnum.getByValue(schedule.getImmediatelyType()));
        task.setUnit(TimeUnit.getByValue(schedule.getUnit()));
        task.setTaskClassPath(schedule.getClassPath());
        task.setStartTime(DateUtils.parse(schedule.getStartTime()));
        task.setEndTime(DateUtils.parse(schedule.getEndTime()));
        //task.setGroup();
        return task;
    }
}
