package com.github.liuche51.easyTaskX.cluster.master;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.task.broker.ReDispatchToClientTask;
import com.github.liuche51.easyTaskX.cluster.task.master.NewMasterSyncBakDataTask;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
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

/**
 * master服务入口
 */
public class MasterService {
    private static final Logger log = LoggerFactory.getLogger(MasterService.class);

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
     * @param index
     * @return
     * @throws SQLException
     */
    public static List<BinlogSchedule> getScheduleBinlogByIndex(long index) throws SQLException {
        return BinlogScheduleDao.getScheduleBinlogByIndex(index, NodeService.getConfig().getAdvanceConfig().getBinlogCount());
    }

}
