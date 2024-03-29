package com.github.liuche51.easyTaskX.cluster.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一次性运行的后台任务基类
 */
public abstract class OnceTask extends Thread{
    private volatile boolean exit = false;
    private volatile boolean finished = false;

    protected boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }
}
