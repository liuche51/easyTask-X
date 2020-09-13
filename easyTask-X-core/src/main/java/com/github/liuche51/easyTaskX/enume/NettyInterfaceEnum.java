package com.github.liuche51.easyTaskX.enume;

public class NettyInterfaceEnum {
    /**
     * 客户端提交任务
     */
    public static final String CLIENT_SUBMIT_TASK="ClientSubmitTask";
    /**
     * 客户端删除任务
     */
    public static final String CLIENT_DELETE_TASK="ClientDeleteTask";
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
     * 获取数据库表信息接口
     */
    public static final String GET_DBINFO_BY_TASKID="GetDBInfoByTaskId";
    /**
     * Follow对Leader的心跳接口
     */
    public static final String Heartbeat="Heartbeat";
    /**
     * 集群leader通知分片follows已经选出新leader
     */
    public static final String NOTIFY_SLICE_FOLLOW_NEW_LEADER="NotifySliceFollowNewLeader";
    /**
     * 集群leader通知分片leader已经选出新follow
     */
    public static final String NotifySliceLeaderVoteNewFollow="NotifySliceLeaderVoteNewFollow";
    /**
     * 通知集群节点获取最新注册表信息
     */
    public static final String NOTIFY_NODE_UPDATE_REGEDIT="NotifyNodeUpdateRegedit";
    /**
     * 分片leader通知集群leader，已经完成对新follow的数据同步。请求更新数据同步状态
     */
    public static final String NotifyClusterLeaderUpdateRegeditForDataStatus="notifyClusterLeaderUpdateRegeditForDataStatus";
    /**
     * broker获取集群leader最新注册表信息
     */
    public static final String UPDATE_REGEDIT="UpdateRegedit";
}
