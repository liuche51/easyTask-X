package com.github.liuche51.easyTaskX.util;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtil {
    protected static final Logger log = LoggerFactory.getLogger(LogUtil.class);

    /**
     * 普通info日志。和系统配置保持一直
     *
     * @param s
     * @param o
     */
    public static void info(String s, Object... o) {
        LogUtil.info(s, o);
    }

    /**
     * error日志。和系统配置保持一直
     *
     * @param s
     * @param o
     */
    public static void error(String s, Object... o) {
        LogUtil.error(s, o);
    }

    /**
     * 专用于详细任务调试时使用。生产上需要关闭。
     *
     * @param s
     * @param o
     */
    public static void debug(String s, Object... o) {
        if (NodeService.getConfig().getAdvanceConfig().isDebug())
            LogUtil.info(s, o);
    }
}
