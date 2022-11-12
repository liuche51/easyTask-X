package com.github.liuche51.easyTaskX.dto.db;

/**
 * 任务跟踪日志
 */
public class TraceLog {
    private String taskid;
    private String content;
    private String createTime;

    public TraceLog(String taskid, String content) {
        this.taskid = taskid;
        this.content = content;
    }

    public String getTaskid() {
        return taskid;
    }

    public void setTaskid(String taskid) {
        this.taskid = taskid;
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
