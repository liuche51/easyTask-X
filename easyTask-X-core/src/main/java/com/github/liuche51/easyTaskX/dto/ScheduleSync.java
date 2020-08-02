package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.core.EasyTaskConfig;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;

public class ScheduleSync {
    private String transactionId;
    private String scheduleId;
    private String follow;
    private short status;
    private String createTime;
    private String modifyTime;

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public String getFollow() {
        return follow;
    }

    public void setFollows(String follows) {
        this.follow = follows;
    }

    public short getStatus() {
        return status;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }
}
