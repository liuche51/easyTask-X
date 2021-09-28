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
    public static final String LeaderNotiySlaveVotedNewMaster="LeaderNotiySlaveVotedNewMaster";
    /**
     * leader通知broker更新注册表信息
     */
    public static final String LeaderNotifyBrokerUpdateRegedit="LeaderNotifyBrokerUpdateRegedit";
    /**
     * leader通知Clinets。Broker发生变更。
     */
    public static final String LeaderNotifyClientBrokerChanged="LeaderNotifyClientBrokerChanged";
    /**
     * leader通知Broker注册成功。
     */
    public static final String LeaderNotifyBrokerRegisterSucceeded="LeaderNotifyBrokerRegisterSucceeded";
    /**
     * leader通知brokers。Client已经变动
     */
    public static final String LeaderNotifyBrokerClientChanged="LeaderNotifyBrokerClientChanged";
    /**
     * leader通知Follow更新备用leader信息
     */
    public static final String LeaderNotifyFollowUpdateBakLeaderInfo="LeaderNotifyFollowUpdateBakLeaderInfo";
    /**
     * leader通知其BakLeader节点更新注册表信息
     */
    public static final String LeaderNotifyBakLeaderUpdateRegedit="LeaderNotifyBakLeaderUpdateRegedit";
    /**
     * BakLeader主动通过定时任务从leader更新注册表
     */
    public static final String BakLeaderRequestLeaderSendRegedit="BakLeaderRequestLeaderSendRegedit";
    /**
     * master通知leader，已经完成对新follow的数据同步。请求更新数据同步状态
     */
    public static final String MasterNotifyLeaderUpdateRegeditForDataStatus="MasterNotifyLeaderUpdateRegeditForDataStatus";
    /**
     * slave请求master获取ScheduleBinlog数据
     */
    public static final String SlaveRequestMasterGetScheduleBinlogData="SlaveRequestMasterGetScheduleBinlogData";
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
    /**
     * Broker通知Client接受执行新任务
     */
    public static final String BrokerNotifyClientExecuteNewTask="BrokerNotifyClientExecuteNewTask";
    /**
     *broker通知leader，已经完成重新分配任务至新client以及salve的数据同步。请求更新数据同步状态
     */
    public static final String BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus="BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus";
}
