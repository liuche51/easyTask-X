package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.*;
import com.github.liuche51.easyTaskX.netty.server.handler.follow.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.MasterNotifyLeaderUpdateRegeditForDataStatusHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.master.SlaveRequestMasterGetScheduleBinlogDataHandler;
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
                //Master to Leader
                put(NettyInterfaceEnum.MasterNotifyLeaderUpdateRegeditForDataStatus,new MasterNotifyLeaderUpdateRegeditForDataStatusHandler());
                //slave to master
                put(NettyInterfaceEnum.SlaveRequestMasterGetScheduleBinlogData,new SlaveRequestMasterGetScheduleBinlogDataHandler());
                put(NettyInterfaceEnum.BakLeaderRequestLeaderGetClusterMetaBinlogData,new BakLeaderRequestLeaderGetClusterMetaBinlogDataHandler());
                //Follow to  leader
                put(NettyInterfaceEnum.FollowHeartbeatToLeader,new FollowHeartbeatToLeaderHandler());
                put(NettyInterfaceEnum.FollowRequestLeaderSendRegedit,new FollowRequestLeaderSendRegeditHandler());
                //Leader to Broker
                put(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit,new LeaderNotifyBrokerUpdateRegeditHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerClientChanged,new LeaderNotifyBrokerClientChangedHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerRegisterSucceeded,new LeaderNotifyBrokerRegisterSucceededHandler());
                //Leader to Slave
                put(NettyInterfaceEnum.LeaderNotiySlaveVotedNewMaster,new LeaderNotiySlaveVotedNewMasterHandler());
                //Leader to Follow
                put(NettyInterfaceEnum.LeaderNotifyFollowUpdateBakLeaderInfo,new LeaderNotifyFollowUpdateBakLeaderHandler());

                //Broker to Leader
                put(NettyInterfaceEnum.BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus,new BrokerNotifyLeaderUpdateRegeditForBrokerReDispatchTaskStatusHandler());
                put(NettyInterfaceEnum.BrokerRequestLeaderSendClients,new BrokerRequestLeaderSendClientsHandler());
                //Monitor
                put(NettyInterfaceEnum.GetDBInfoByTaskId,new GetDBInfoByTaskIdHandler());
            }
        };
    }
    public abstract ByteString process(Dto.Frame frame) throws Exception;
}
