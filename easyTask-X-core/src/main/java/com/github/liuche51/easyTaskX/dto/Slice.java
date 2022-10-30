package com.github.liuche51.easyTaskX.dto;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 时间秒的分片任务集合
 */
public class Slice {
    /**
     * 任务容器。
     */
    private ConcurrentHashMap<String, InnerTask> list=new ConcurrentHashMap<String, InnerTask>();;

    public ConcurrentHashMap<String, InnerTask> getList() {
        return list;
    }

    public void setList(ConcurrentHashMap<String, InnerTask> list) {
        this.list = list;
    }
}
