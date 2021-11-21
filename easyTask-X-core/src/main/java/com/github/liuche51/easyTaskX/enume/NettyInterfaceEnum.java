package com.github.liuche51.easyTaskX.enume;

public class NettyInterfaceEnum {
    /**
     * 客户端提交任务
     */
    public static final String ClientSubmitTaskToBroker="ClientSubmitTaskToBroker";
    /**
     * 客户端内部删除任务
     */
    public static final String ClientNotifyBrokerDeleteTask="ClientNotifyBrokerDeleteTask";
    /**
     * 客户端全局通缉模式删除任务
     */
    public static final String ClientNotifyLeaderDeleteTask="ClientNotifyLeaderDeleteTask";
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
     * master通知leader，变更slave与master数据同步状态
     */
    public static final String MasterNotifyLeaderChangeSlaveDataStatus="MasterNotifyLeaderChangeSlaveDataStatus";
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
     * slave请求master获取ScheduleBinlog数据
     */
    public static final String SlaveRequestMasterGetScheduleBinlogData="SlaveRequestMasterGetScheduleBinlogData";
    /**
     * slave通知Master，已经同步了还不能使用的任务。
     */
    public static final String SlaveNotifyMasterHasSyncUnUseTask="SlaveNotifyMasterHasSyncUnUseTask";
    /**
     * bakleader请求leader获取ClusterMetaBinlog数据
     */
    public static final String BakLeaderRequestLeaderGetClusterMetaBinlogData="BakLeaderRequestLeaderGetClusterMetaBinlogData";
    /**
     * bakleader查询其他bakleader当前的数据状态
     */
    public static final String BakLeaderQueryOtherBakLeaderDataStatus="BakLeaderQueryOtherBakLeaderDataStatus";
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
     * Broker通知Client反馈提交的任务状态信息
     */
    public static final String BrokerNotifyClientSubmitTaskResult="BrokerNotifyClientSubmitTaskResult";
    /**
     *broker通知leader，已经完成重新分配任务至新client以及salve的数据同步。请求更新数据同步状态
     */
    public static final String BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus="BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus";
}
