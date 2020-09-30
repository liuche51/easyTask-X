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
     * master同步任务数据给新slave备份接口
     */
    public static final String MasterSyncDataToNewSlave="MasterSyncDataToNewSlave";
    /**
     * 获取数据库表信息接口
     */
    public static final String GetDBInfoByTaskId="GetDBInfoByTaskId";
    /**
     * Follow对Leader的心跳接口
     */
    public static final String Heartbeat="Heartbeat";
    /**
     * leader通知slaves已经选出新master
     */
    public static final String NotifySlaveNewLeader="NotifySlaveNewLeader";
    /**
     * leader通知master已经选出新Slave
     */
    public static final String NotifyMasterVoteNewSlave="NotifyMasterVoteNewSlave";
    /**
     * leader通知client更新注册表信息
     */
    public static final String LeaderNotifyClientUpdateRegedit="LeaderNotifyClientUpdateRegedit";
    /**
     * leader通知broker更新注册表信息
     */
    public static final String LeaderNotifyBrokerUpdateRegedit="LeaderNotifyBrokerUpdateRegedit";
    /**
     * leader通知其slave节点更新注册表信息
     */
    public static final String LeaderNotifySalveUpdateRegedit="LeaderNotifySalveUpdateRegedit";
    /**
     * 集群slave主动通过定时任务从leader更新注册表
     */
    public static final String SalveRequestUpdateClusterRegedit="SalveRequestUpdateClusterRegedit";
    /**
     * master通知leader，已经完成对新follow的数据同步。请求更新数据同步状态
     */
    public static final String NotifyLeaderUpdateRegeditForDataStatus="notifyLeaderUpdateRegeditForDataStatus";
    /**
     * broker或client通过定时任务获取leader最新注册表信息
     */
    public static final String FollowRequestUpdateRegedit="FollowRequestUpdateRegedit";
}
