package com.github.liuche51.easyTaskX.cluster.task;


import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 同步与其他关联节点的时钟差定时任务
 */
public class NodeClockAdjustTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                }
                ConcurrentHashMap<String, Node> follows = ClusterService.CURRENTNODE.getFollows();
                Iterator<Map.Entry<String, Node>> items2 = follows.entrySet().iterator();
                while (items2.hasNext()) {
                    Map.Entry<String, Node> item = items2.next();
                }
            } catch (ConcurrentModificationException e) {
                //多线程并发导致items.next()异常，但是没啥太大影响(影响后续元素迭代)。可以直接忽略
                log.error("normally exception error.can ignore." + e.getMessage());
            } catch (Exception e) {
                log.error("submitNewTaskByOldLeader()->", e);
            } finally {
                try {
                    Thread.sleep(300000l);//5分钟执行一次
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
        }

    }
}
