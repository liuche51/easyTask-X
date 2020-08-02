package com.github.liuche51.easyTaskX.enume;

public class NettyInterfaceEnum {
    /**
     * 预备提交任务接口。阶段一
     */
    public static final String TRAN_TRYSAVETASK="Tran_TrySaveTask";
    /**
     * 确认提交任务接口。阶段二
     */
    public static final String TRAN_CONFIRMSAVETASK="Tran_ConfirmSaveTask";
    /**
     * 取消任务接口。事务回滚
     */
    public static final String TRAN_CANCELSAVETASK="Tran_CancelSaveTask";
    /**
     * 预备删除任务接口。阶段一
     */
    public static final String TRAN_TRYDELTASK="Tran_TryDelTask";
    /**
     * 确认删除任务接口。阶段二
     */
    public static final String TRAN_CONFIRMDELTASK="Tran_ConfirmDelTask";
    /**
     * 取消任务接口。事务回滚
     */
    public static final String TRAN_CANCELDELTASK="Tran_CancelDelTask";
    /**
     * leader同步任务数据给新follow备份接口
     */
    public static final String LEADER_SYNC_DATA_TO_NEW_FOLLOW="LeaderSyncDataToNewFollow";
    /**
     * 同步leader位置信息接口
     */
    public static final String SYNC_LEADER_POSITION="SyncLeaderPosition";
    /**
     * 获取数据库表信息接口
     */
    public static final String GET_DBINFO_BY_TASKID="GetDBInfoByTaskId";
}
