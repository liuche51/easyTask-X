package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * 分片Leader服务入口
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

    /**
     * 分片leader通知集群leader，已经完成对新follow的数据同步。请求更新数据同步状态
     */
    public static void notifyClusterLeaderUpdateRegeditForDataStatus(String followAddress,String dataStatus){
        ClusterService.getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder=Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setBody(NettyInterfaceEnum.NotifyClusterLeaderUpdateRegeditForDataStatus)
                            .setSource(ClusterService.CURRENTNODE.getAddress()).setBody(followAddress+"|"+dataStatus);
                    NettyMsgService.sendSyncMsgWithCount(builder,ClusterService.CURRENTNODE.getClusterLeader().getClient(),ClusterService.getConfig().getTryCount(),5,null);
                }catch (Exception e){
                    log.error("notifyClusterLeaderUpdateRegeditForDataStatus()->exception!",e);
                }
            }
        });
    }

}
