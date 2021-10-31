package com.github.liuche51.easyTaskX.dto;

import com.github.liuche51.easyTaskX.util.StringConstant;

/**
 * 提交的任务结果数据分装对象
 */
public class SubmitTaskResult {
    private String id;
    /**
     * 任务状态。
     * 0等待反馈，1反馈成功，9反馈失败。
     */
    private int status = 0;
    private String error = StringConstant.EMPTY;

    public SubmitTaskResult() {
    }

    public SubmitTaskResult(String id, int status) {
        this.id = id;
        this.status = status;
    }

    public SubmitTaskResult(String id, int status, String error) {
        this.id = id;
        this.status = status;
        this.error = error;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
