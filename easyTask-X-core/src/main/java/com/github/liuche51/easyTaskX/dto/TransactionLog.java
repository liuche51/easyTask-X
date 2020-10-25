package com.github.liuche51.easyTaskX.dto;

public class TransactionLog {
    private String id;
    private String content;
    private String tableName;
    private short type;
    private short status;
    private String slaves;//重试事务所需的slave地址
    private String retryTime;
    private short retryCount;
    private String createTime;
    private String modifyTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public short getType() {
        return type;
    }

    public void setType(short type) {
        this.type = type;
    }

    public short getStatus() {
        return status;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public String getSlaves() {
        return slaves;
    }

    public void setSlaves(String slaves) {
        this.slaves = slaves;
    }

    public String getRetryTime() {
        return retryTime;
    }

    public void setRetryTime(String retryTime) {
        this.retryTime = retryTime;
    }

    public short getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(short retryCount) {
        this.retryCount = retryCount;
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
