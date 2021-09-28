package com.github.liuche51.easyTaskX.dto.db;

/**
 * 任务表BinLog
 */
public class BinlogSchedule {
    private Long id;
    private String sql;
    private String createTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
