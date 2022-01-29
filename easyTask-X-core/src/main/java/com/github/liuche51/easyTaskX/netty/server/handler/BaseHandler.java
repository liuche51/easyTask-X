package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.broker.*;
import com.github.liuche51.easyTaskX.netty.server.handler.follow.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.*;
import com.github.liuche51.easyTaskX.netty.server.handler.master.SlaveNotifyMasterHasSyncUnUseTaskHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.master.SlaveRequestMasterGetScheduleBinlogDataHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.slave.*;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandler {
    public static Map<String,BaseHandler> INSTANCES;
    static {
        INSTANCES=new HashMap<String,BaseHandler>(){
            {
                //Client to Broker
                put(NettyInterfaceEnum.ClientSubmitTaskToBroker,new ClientSubmitTaskToBrokerHandler());
                put(NettyInterfaceEnum.ClientNotifyBrokerDeleteTask,new ClientNotifyBrokerDeleteTaskHandler());
                //Client to Leader
                put(NettyInterfaceEnum.ClientRequestLeaderSendBrokers,new ClientRequestLeaderSendBrokersHandler());
                put(NettyInterfaceEnum.ClientNotifyLeaderDeleteTask,new ClientNotifyLeaderDeleteTaskHandler());
                //slave to master
                put(NettyInterfaceEnum.SlaveRequestMasterGetScheduleBinlogData,new SlaveRequestMasterGetScheduleBinlogDataHandler());
                put(NettyInterfaceEnum.BakLeaderRequestLeaderGetClusterMetaBinlogData,new BakLeaderRequestLeaderGetClusterMetaBinlogDataHandler());
                put(NettyInterfaceEnum.BakLeaderQueryOtherBakLeaderDataStatus,new BakLeaderQueryOtherBakLeaderDataStatusHandler());
                put(NettyInterfaceEnum.SlaveNotifyMasterHasSyncUnUseTask,new SlaveNotifyMasterHasSyncUnUseTaskHandler());
                //Follow to  leader
                put(NettyInterfaceEnum.FollowHeartbeatToLeader,new FollowHeartbeatToLeaderHandler());
                put(NettyInterfaceEnum.FollowRequestLeaderSendRegedit,new FollowRequestLeaderSendRegeditHandler());
                put(NettyInterfaceEnum.FollowNotifyLeaderHasRestart,new FollowNotifyLeaderHasRestartHandler());
                put(NettyInterfaceEnum.FollowRequestLeaderGetRegNodeStatus,new FollowRequestLeaderGetRegNodeStatusHandler());
                //Leader to Broker
                put(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit,new LeaderNotifyBrokerUpdateRegeditHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerClientChanged,new LeaderNotifyBrokerClientChangedHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerRegisterSucceeded,new LeaderNotifyBrokerRegisterSucceededHandler());
                //Leader to Slave
                put(NettyInterfaceEnum.LeaderNotiySlaveVotedNewMaster,new LeaderNotiySlaveVotedNewMasterHandler());
                //Leader to Follow
                put(NettyInterfaceEnum.LeaderNotifyFollowUpdateBakLeaderInfo,new LeaderNotifyFollowUpdateBakLeaderHandler());

                put(NettyInterfaceEnum.BrokerRequestLeaderSendClients,new BrokerRequestLeaderSendClientsHandler());
                put(NettyInterfaceEnum.BrokerNotifyLeaderChangeRegNodeStatus,new BrokerNotifyLeaderChangeRegNodeStatusHandler());
                //Monitor
                put(NettyInterfaceEnum.GetDBInfoByTaskId,new GetDBInfoByTaskIdHandler());
            }
        };
    }
    public abstract ByteString process(Dto.Frame frame) throws Exception;
}
