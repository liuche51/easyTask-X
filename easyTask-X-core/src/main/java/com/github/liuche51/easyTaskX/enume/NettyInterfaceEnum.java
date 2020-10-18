package com.github.liuche51.easyTaskX.enume;

public class NettyInterfaceEnum {
    /**
     * 客户端提交任务
     */
    public static final String ClientNotifyBrokerSubmitTask="ClientNotifyBrokerSubmitTask";
    /**
     * 客户端删除任务
     */
    public static final String ClientNotifyBrokerDeleteTask="ClientNotifyBrokerDeleteTask";
    /**
     * 预备提交任务接口。阶段一
     */
    public static final String MasterNotifySlaveTranTrySaveTask="MasterNotifySlaveTranTrySaveTask";
    /**
     * 确认提交任务接口。阶段二
     */
    public static final String MasterNotifySlaveTranConfirmSaveTask="MasterNotifySlaveTranConfirmSaveTask";
    /**
     * 取消任务接口。事务回滚
     */
    public static final String MasterNotifySlaveTranCancelSaveTask="MasterNotifySlaveTranCancelSaveTask";
    /**
     * 预备删除任务接口。阶段一
     */
    public static final String MasterNotifySlaveTranTryDelTask="MasterNotifySlaveTranTryDelTask";
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
    public static final String FollowHeartbeatToLeader="FollowHeartbeatToLeader";
    /**
     * leader通知slaves已经选出新master
     */
    public static final String NotifySlaveNewLeader="NotifySlaveNewLeader";
    /**
     * leader通知master已经选出新Slave
     */
    public static final String LeaderNotifyMasterVoteNewSlave="LeaderNotifyMasterVoteNewSlave";
    /**
     * leader通知client更新注册表信息
     */
    public static final String LeaderNotifyClientUpdateRegedit="LeaderNotifyClientUpdateRegedit";
    /**
     * leader通知broker更新注册表信息
     */
    public static final String LeaderNotifyBrokerUpdateRegedit="LeaderNotifyBrokerUpdateRegedit";
    /**
     * leader通知clients更新Broker列表变动信息
     */
    public static final String LeaderNotifyClientUpdateBrokerChange="LeaderNotifyClientUpdateRegedit";
    /**
     * leader通知brokers更新Client列表变动信息
     */
    public static final String LeaderNotifyBrokersUpdateClientChange="LeaderNotifyBrokerUpdateRegedit";
    /**
     * leader通知Follow更新备用leader信息
     */
    public static final String LeaderNotifyFollwUpdateBakLeaderInfo="LeaderNotifyFollwUpdateBakLeaderInfo";
    /**
     * leader通知其slave节点更新注册表信息
     */
    public static final String LeaderNotifySalveUpdateRegedit="LeaderNotifySalveUpdateRegedit";
    /**
     * 集群slave主动通过定时任务从leader更新注册表
     */
    public static final String ClusterSlaveRequestLeaderSendRegedit="ClusterSlaveRequestLeaderSendRegedit";
    /**
     * master通知leader，已经完成对新follow的数据同步。请求更新数据同步状态
     */
    public static final String MasterNotifyLeaderUpdateRegeditForDataStatus="MasterNotifyLeaderUpdateRegeditForDataStatus";
    /**
     * broker或client通过定时任务获取leader最新注册表信息
     */
    public static final String FollowRequestLeaderSendRegedit="FollowRequestLeaderSendRegedit";
    /**
     * Client请求更新Broker列表信
     */
    public static final String ClientRequestLeaderSendBrokers="ClientRequestLeaderSendBrokers";
    /**
     * Broker请求更新Client列表信
     */
    public static final String BrokerRequestLeaderSendClients="BrokerRequestLeaderSendClients";
}
