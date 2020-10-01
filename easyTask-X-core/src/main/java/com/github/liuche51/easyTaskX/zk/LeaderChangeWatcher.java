package com.github.liuche51.easyTaskX.zk;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * Leader节点变化监听处理逻辑
 * 一次性监听使用。目前暂不使用
 */
public class LeaderChangeWatcher implements CuratorWatcher {
    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        System.out.println("触发watcher，节点路径为：" + watchedEvent.getPath());
    }
}
