package com.github.liuche51.easyTaskX.ext;

/**
 * 任务日志跟踪外部扩展接口
 * 1、用户需要将任务跟踪日志写入外部系统时实现该接口，并注册到系统配置类中
 */
public interface TaskTraceExt {
    void trace(String taskId,String nodeAddress, String s, Object... o);
}
