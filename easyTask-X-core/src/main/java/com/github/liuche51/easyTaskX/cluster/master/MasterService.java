package com.github.liuche51.easyTaskX.cluster.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.broker.ReDispatchToClientTask;
import com.github.liuche51.easyTaskX.cluster.task.master.NewMasterSyncBakDataTask;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.task.*;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * master服务入口
 */
public class MasterService {
    private static final Logger log = LoggerFactory.getLogger(MasterService.class);
    /**
     * 任务同步到slave状态记录
     * 1、高可靠模式下使用。key=任务ID，value=是否已经有salve同步任务
     * 2、salve同步任务使用binlog方式同步
     */
    public static ConcurrentHashMap<String, Boolean> TASK_SYNC_SALVE_STATUS = new ConcurrentHashMap<>();

    /**
     * 新Master将失效的旧Master的备份任务数据删除掉
     *
     * @param oldMasterAddress
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void deleteOldMasterBackTask(String oldMasterAddress) throws SQLException, ClassNotFoundException {
        ScheduleBakDao.deleteBySource(oldMasterAddress);
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
     * 查询指定数据的binlog数据
     *
     * @param index
     * @return
     * @throws SQLException
     */
    public static List<BinlogSchedule> getScheduleBinlogByIndex(long index) throws SQLException {
        List<BinlogSchedule> binlogSchedules = BinlogScheduleDao.getScheduleBinlogByIndex(index, NodeService.getConfig().getAdvanceConfig().getBinlogCount());
        return binlogSchedules;
    }

    /**
     * master通知leader，变更slave与master数据同步状态
     *
     * @param dataStatus 1已完成同步。0同步中
     */
    public static void notifyNotifyLeaderChangeDataStatus(String salve, String dataStatus) {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        try {
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.MasterNotifyLeaderChangeSlaveDataStatus).setSource(NodeService.getConfig().getAddress())
                    .setBody(salve + StringConstant.CHAR_SPRIT_STRING + dataStatus);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENTNODE.getClusterLeader().getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (!ret) {
                NettyMsgService.writeRpcErrorMsgToDb("master通知leader变更salve与master数据同步状态。失败！", "com.github.liuche51.easyTaskX.cluster.master.MasterService.notifyNotifyLeaderChangeDataStatus");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
