package com.github.liuche51.easyTaskX.cluster.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.broker.ReDispatchToClientTask;
import com.github.liuche51.easyTaskX.cluster.task.master.NewMasterSyncBakDataTask;
import com.github.liuche51.easyTaskX.cluster.task.master.SyncDataToNewSlaveTask;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * master服务入口
 */
public class MasterService {
    private static final Logger log = LoggerFactory.getLogger(MasterService.class);

    /**
     * 将失效的leader的备份任务数据删除掉
     *
     * @param oldLeaderAddress
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void deleteOldLeaderBackTask(String oldLeaderAddress) throws SQLException, ClassNotFoundException {
        ScheduleBakDao.deleteBySource(oldLeaderAddress);
    }

    /**
     * master同步数据到新Slave
     * 目前设计为只有一个线程同步给某个Slave
     *
     * @param oldSlave
     * @param newSlave
     */
    public static synchronized OnceTask syncDataToNewSlave(Node oldSlave, Node newSlave) {
        SyncDataToNewSlaveTask task = new SyncDataToNewSlaveTask(oldSlave, newSlave);
        String key = task.getClass().getName() + "," + oldSlave.getAddress();
        if (ReDispatchToClientTask.runningTask.contains(key)) return null;
        ReDispatchToClientTask.runningTask.put(key, null);
        task.start();
        NodeService.onceTasks.add(task);
        return task;
    }

    /**
     * 新master将旧master的备份数据同步给自己的slave
     * 后期需要考虑数据一致性
     *
     * @param oldMasterAddress
     */
    public static synchronized OnceTask submitNewTaskByOldMaster(String oldMasterAddress) {
        NewMasterSyncBakDataTask task = new NewMasterSyncBakDataTask(oldMasterAddress);
        String key = task.getClass().getName() + "," + oldMasterAddress;
        if (ReDispatchToClientTask.runningTask.contains(key)) return null;
        ReDispatchToClientTask.runningTask.put(key, null);
        task.start();
        NodeService.onceTasks.add(task);
        return task;
    }

    /**
     * master通知leader，已经完成对新follow的数据同步。请求更新数据同步状态
     */
    public static void notifyClusterLeaderUpdateRegeditForDataStatus(String followAddress, String dataStatus) {
        NodeService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setBody(NettyInterfaceEnum.MasterNotifyLeaderUpdateRegeditForDataStatus)
                            .setSource(NodeService.CURRENTNODE.getAddress()).setBody(followAddress + StringConstant.CHAR_SPRIT_STRING + dataStatus);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyClusterLeaderUpdateRegeditForDataStatus() failed.");
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        });
    }

}
