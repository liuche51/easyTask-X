package com.github.liuche51.easyTaskX.dto.db;

import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringUtils;

import java.time.ZonedDateTime;

public class ScheduleBak {
    private String id;
    private String classPath;
    private long executeTime;
    private String taskType;
    private long period;
    private String unit;
    private String param;
    private String source;
    private String transactionId;
    private String createTime;
    private String modifyTime;
    /**
     * 任务状态。1正常，0暂时不可用
     */
    private int status;
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getCreateTime() {
        return createTime;
    }
    public void setCreateTime(String createTime) {
        this.createTime = createTime;
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

    public static ScheduleBak valueOf(ScheduleDto.Schedule dto){
        ScheduleBak schedule=new ScheduleBak();
        schedule.id=dto.getId();
        schedule.classPath=dto.getClassPath();
        schedule.executeTime=dto.getExecuteTime();
        schedule.taskType=dto.getTaskType();
        schedule.period=dto.getPeriod();
        schedule.unit=dto.getUnit();
        schedule.param=dto.getParam();
        schedule.source=dto.getSource();
        schedule.status=dto.getStatus();
        schedule.startTime= DateUtils.parse(dto.getStartTime());
        schedule.endTime= DateUtils.parse(dto.getEndTime());
        return schedule;
    }
}
