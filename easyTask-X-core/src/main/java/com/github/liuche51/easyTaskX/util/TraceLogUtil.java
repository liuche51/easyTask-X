package com.github.liuche51.easyTaskX.util;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.db.TraceLog;
import com.github.liuche51.easyTaskX.ext.TaskTraceExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import com.github.liuche51.easyTaskX.exception.*;
/**
 * 任务跟踪日志专用工具类
 */
public class TraceLogUtil {
    protected static final Logger log = LoggerFactory.getLogger(TraceLogUtil.class);
    /**
     * 本地任务跟踪日志
     */
    public static ConcurrentHashMap<String, List<String>> TASK_TRACE_LOGS=new ConcurrentHashMap<>();
    /**
     * 等待写本地库的跟踪日志
     */
    public static LinkedBlockingQueue<TraceLog> TASK_TRACE_WAITTO_WRITEDB=new LinkedBlockingQueue<>(BrokerService.getConfig().getAdvanceConfig().getTaskQueueCapacity());
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
                FormattingTuple ft = MessageFormatter.arrayFormat(s, o);
                TraceLog traceLog=new TraceLog(taskId,nodeAddress+" "+ft.getMessage());
                // 如果日志缓冲队列已满，则丢弃，不阻塞主线程。写入重要系统错误
                if(!TASK_TRACE_WAITTO_WRITEDB.offer(traceLog)){
                    ImportantErrorLogUtil.writeQueueErrorMsgToDb("队列LogUtil.TASK_TRACE_WAITTO_WRITEDB.", "com.github.liuche51.easyTaskX.util.LogUtil.trace");
                }
            }
        }
    }
}
