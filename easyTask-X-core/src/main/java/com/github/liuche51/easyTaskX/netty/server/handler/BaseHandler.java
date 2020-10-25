package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.ClientNotifyBrokerDeleteTaskHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.ClientNotifyBrokerSubmitTaskHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.LeaderNotifyBrokerClientChangedHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.follow.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.MasterNotifyLeaderUpdateRegeditForDataStatusHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.LeaderNotifyBrokerUpdateRegeditHandler;
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
                put(NettyInterfaceEnum.ClientNotifyBrokerSubmitTask,new ClientNotifyBrokerSubmitTaskHandler());
                put(NettyInterfaceEnum.ClientNotifyBrokerDeleteTask,new ClientNotifyBrokerDeleteTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranTrySaveTask,new MasterNotifySlaveTranTrySaveTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranConfirmSaveTask,new MasterNotifySlaveTranConfirmSaveTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranCancelSaveTask,new MasterNotifySlaveTranCancelSaveTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranTryDelTask,new MasterNotifySlaveTranTryDelTaskHandler());
                put(NettyInterfaceEnum.MasterNotifySlaveTranTryUpdateTask,new MasterNotifySlaveTranTryUpdateTaskHandler());

                put(NettyInterfaceEnum.MasterSyncDataToNewSlave,new MasterSyncDataToNewSlaveHandler());
                put(NettyInterfaceEnum.GetDBInfoByTaskId,new GetDBInfoByTaskIdHandler());
                put(NettyInterfaceEnum.FollowHeartbeatToLeader,new FollowHeartbeatToLeaderHandler());
                put(NettyInterfaceEnum.FollowRequestLeaderSendRegedit,new FollowRequestLeaderSendRegeditHandler());
                //Leader
                put(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit,new LeaderNotifyBrokerUpdateRegeditHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerClientChanged,new LeaderNotifyBrokerClientChangedHandler());
                put(NettyInterfaceEnum.LeaderNotifyMasterVoteNewSlave,new LeaderNotifyMasterVoteNewSlaveHandler());
                put(NettyInterfaceEnum.MasterNotifyLeaderUpdateRegeditForDataStatus,new MasterNotifyLeaderUpdateRegeditForDataStatusHandler());
                put(NettyInterfaceEnum.LeaderNotifyFollwUpdateBakLeaderInfo,new LeaderNotifyFollowUpdateBakLeaderHandler());
                put(NettyInterfaceEnum.LeaderNotifySalveUpdateRegedit,new LeaderNotifySalveUpdateRegeditHandler());
                put(NettyInterfaceEnum.ClusterSlaveRequestLeaderSendRegedit,new ClusterSlaveRequestLeaderSendRegeditHandler());
                put(NettyInterfaceEnum.ClientRequestLeaderSendBrokers,new ClientRequestLeaderSendBrokersHandler());
                put(NettyInterfaceEnum.BrokerRequestLeaderSendClients,new BrokerRequestLeaderSendClientsHandler());
            }
        };
    }
    public abstract ByteString process(Dto.Frame frame) throws Exception;
}
