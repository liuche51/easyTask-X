package com.github.liuche51.easyTaskX.util;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.ext.TaskTraceExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.github.liuche51.easyTaskX.exception.*;

public class LogUtil {
    protected static final Logger log = LoggerFactory.getLogger(LogUtil.class);
    /**
     * 本地任务跟踪日志
     */
    public static ConcurrentHashMap<String, List<String>> TASK_TRACE_LOGS=new ConcurrentHashMap<>();
    /**
     * 普通info日志。和系统配置保持一直
     *
     * @param s
     * @param o
     */
    public static void info(String s, Object... o) {
        log.info(s, o);
    }

    /**
     * error日志。和系统配置保持一直
     *
     * @param s
     * @param o
     */
    public static void error(String s, Object... o) {
        log.error(s, o);
    }

    /**
     * 专用于详细任务调试时使用。生产上需要关闭。
     *
     * @param s
     * @param o
     */
    public static void debug(String s, Object... o) {
        if (BrokerService.getConfig().getAdvanceConfig().isDebug())
            log.info(s, o);
    }
    /**
     * 任务跟踪日志专用
     * @param taskId
     * @param s
     * @param o
     */
    public static void trace(String taskId, String s, Object... o) {
        if (BrokerService.getConfig().getAdvanceConfig().isDebug()) {
            String nodeAddress="";
            try {
                nodeAddress=BrokerService.getConfig().getAddress();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (null == BrokerService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())
                log.info("TaskId=" + taskId + ":" + s, o);
            else if("memory".equalsIgnoreCase(BrokerService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                List<String> logs = TASK_TRACE_LOGS.get(taskId);
                if(logs==null){
                    logs=new LinkedList<>();
                    TASK_TRACE_LOGS.put(taskId,logs);
                }
                FormattingTuple ft = MessageFormatter.arrayFormat(s, o);
                logs.add("happened at "+nodeAddress+" "+ft.getMessage());
            }else if("ext".equalsIgnoreCase(BrokerService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                TaskTraceExt taskTraceExt = BrokerService.getConfig().getAdvanceConfig().getTaskTraceExt();
                if(taskTraceExt!=null){
                    taskTraceExt.trace(taskId,nodeAddress,s,o);
                }else{
                    try {
                        throw new EasyTaskException(ExceptionCode.TaskTraceExt_NotFind,"config item taskTraceExt not find.");
                    } catch (EasyTaskException e) {
                        log.error(e.getMessage());
                    }
                }

            }else if("local".equalsIgnoreCase(BrokerService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                // Todo 暂时不支持客户端本地存储日志，需要发送到服务端存储
            }
        }
    }
}
