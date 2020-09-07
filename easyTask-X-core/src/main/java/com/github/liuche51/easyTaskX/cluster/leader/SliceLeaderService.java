package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Leader服务入口
 */
public class SliceLeaderService {
    private static final Logger log = LoggerFactory.getLogger(SliceLeaderService.class);

    /**
     * 将失效的leader的备份任务数据删除掉
     * @param oldLeaderAddress
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void deleteOldLeaderBackTask(String oldLeaderAddress) throws SQLException, ClassNotFoundException {
        ScheduleBakDao.deleteBySource(oldLeaderAddress);
    }

    /**
     * 节点对zk的心跳。2s一次
     */
    public static TimerTask initHeartBeatToClusterLeader() {
        HeartbeatsTask task=new HeartbeatsTask();
        task.start();
       return task;
    }

    /**
     * 节点对zk的心跳。检查follows是否失效。
     * 失效则进入选举
     */
    public static TimerTask initCheckFollowAlive() {
        CheckFollowsAliveTask task=new CheckFollowsAliveTask();
        task.start();
        return task;
    }

    /**
     * leader同步数据到新follow
     * 目前设计为只有一个线程同步给某个follow
     *
     * @param oldFollow
     * @param newFollow
     */
    public static OnceTask syncDataToNewFollow(Node oldFollow, Node newFollow) {
        SyncDataToNewFollowTask task=new SyncDataToNewFollowTask(oldFollow,newFollow);
        task.start();
        ClusterService.onceTasks.add(task);
        return task;
    }

    /**
     * 新leader将旧leader的备份数据同步给自己的follow
     * 后期需要考虑数据一致性
     *
     * @param oldLeaderAddress
     */
    public static OnceTask submitNewTaskByOldLeader(String oldLeaderAddress) {
        NewLeaderSyncBakDataTask task=new NewLeaderSyncBakDataTask(oldLeaderAddress);
        task.start();
        ClusterService.onceTasks.add(task);
        return task;
    }

}
