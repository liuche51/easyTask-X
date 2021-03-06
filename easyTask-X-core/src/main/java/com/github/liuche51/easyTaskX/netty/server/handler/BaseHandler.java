package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.*;
import com.github.liuche51.easyTaskX.netty.server.handler.follow.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.MasterNotifyLeaderUpdateRegeditForDataStatusHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.master.LeaderNotifyMasterVoteNewSlaveHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.slave.*;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandler {
    public static Map<String,BaseHandler> INSTANCES;
    static {
        INSTANCES=new HashMap<String,BaseHandler>(){
            {
                //Client to Broker
                put(NettyInterfaceEnum.ClientNotifyBrokerSubmitTask,new ClientNotifyBrokerSubmitTaskHandler());
                put(NettyInterfaceEnum.ClientNotifyBrokerDeleteTask,new ClientNotifyBrokerDeleteTaskHandler());
                //Client to Leader
                put(NettyInterfaceEnum.ClientRequestLeaderSendBrokers,new ClientRequestLeaderSendBrokersHandler());
                //Master to Slave
                put(NettyInterfaceEnum.MasterNotifySlaveTranTrySaveTask,new MasterNotifySlaveTranTrySaveTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranConfirmSaveTask,new MasterNotifySlaveTranConfirmSaveTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranCancelSaveTask,new MasterNotifySlaveTranCancelSaveTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranTryDelTask,new MasterNotifySlaveTranTryDelTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranTryUpdateTask,new MasterNotifySlaveTranTryUpdateTaskHandler());
                put(NettyInterfaceEnum.MasterSyncDataToNewSlave,new MasterSyncDataToNewSlaveHandler());
                //Master to Leader
                put(NettyInterfaceEnum.MasterNotifyLeaderUpdateRegeditForDataStatus,new MasterNotifyLeaderUpdateRegeditForDataStatusHandler());

                //Follow to  leader
                put(NettyInterfaceEnum.FollowHeartbeatToLeader,new FollowHeartbeatToLeaderHandler());
                put(NettyInterfaceEnum.FollowRequestLeaderSendRegedit,new FollowRequestLeaderSendRegeditHandler());
                //Leader to Broker
                put(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit,new LeaderNotifyBrokerUpdateRegeditHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerClientChanged,new LeaderNotifyBrokerClientChangedHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerRegisterSucceeded,new LeaderNotifyBrokerRegisterSucceededHandler());
                //Leader to Master
                put(NettyInterfaceEnum.LeaderNotifyMasterVoteNewSlave,new LeaderNotifyMasterVoteNewSlaveHandler());
                //Leader to Slave
                put(NettyInterfaceEnum.LeaderNotiySlaveVotedNewMaster,new LeaderNotiySlaveVotedNewMasterHandler());
                //Leader to Follow
                put(NettyInterfaceEnum.LeaderNotifyFollowUpdateBakLeaderInfo,new LeaderNotifyFollowUpdateBakLeaderHandler());
                //Leader to BakLeader
                put(NettyInterfaceEnum.LeaderNotifyBakLeaderUpdateRegedit,new LeaderNotifyBakLeaderUpdateRegeditHandler());
                //BakLeader to Leader
                put(NettyInterfaceEnum.BakLeaderRequestLeaderSendRegedit,new ClusterSlaveRequestLeaderSendRegeditHandler());
                put(NettyInterfaceEnum.BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus,new BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatusHandler());

                //Broker to Leader
                //put(NettyInterfaceEnum.BrokerNotifyClientExecuteNewTask,new BrokerRequestLeaderSendClientsHandler());
                put(NettyInterfaceEnum.BrokerRequestLeaderSendClients,new BrokerRequestLeaderSendClientsHandler());
                //Monitor
                put(NettyInterfaceEnum.GetDBInfoByTaskId,new GetDBInfoByTaskIdHandler());
            }
        };
    }
    public abstract ByteString process(Dto.Frame frame) throws Exception;
}
