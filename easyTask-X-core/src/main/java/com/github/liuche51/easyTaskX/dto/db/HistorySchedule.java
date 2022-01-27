package com.github.liuche51.easyTaskX.dto.db;

/**
 * 任务表历史
 */
public class HistorySchedule {
    private String id;
    private String content;
    private String createTime;

    public HistorySchedule() {
    }

    public HistorySchedule(String id, String content) {
        this.id = id;
        this.content = content;
    }

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

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
