package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.follow.*;
import com.github.liuche51.easyTaskX.netty.server.handler.leader.*;
import com.github.liuche51.easyTaskX.netty.server.handler.notify.NotifyLeaderUpdateRegeditForDataStatusHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.notify.LeaderNotifyBrokerUpdateRegeditHandler;
import com.github.liuche51.easyTaskX.netty.server.handler.notify.NotifyMasterVoteNewSlaveHandler;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandler {
    public static Map<String,BaseHandler> INSTANCES;
    static {
        INSTANCES=new HashMap<String,BaseHandler>(){
            {
                put(NettyInterfaceEnum.CLIENT_SUBMIT_TASK,new ClientSubmitTaskHandler());
                put(NettyInterfaceEnum.CLIENT_DELETE_TASK,new ClientDeleteTaskHandler());
                put(NettyInterfaceEnum.TRAN_TRYSAVETASK,new TranTrySaveTaskHandler());
                put(NettyInterfaceEnum.TRAN_CONFIRMSAVETASK,new TranConfirmSaveTaskHandler());
                put(NettyInterfaceEnum.TRAN_CANCELSAVETASK,new TranCancelSaveTaskHandler());
                put(NettyInterfaceEnum.TRAN_TRYDELTASK,new TranTryDelTaskHandler());
                put(NettyInterfaceEnum.MasterSyncDataToNewSlave,new MasterSyncDataToNewSlaveHandler());
                put(NettyInterfaceEnum.GetDBInfoByTaskId,new GetDBInfoByTaskIdHandler());
                put(NettyInterfaceEnum.Heartbeat,new HeartbeatHandler());
                put(NettyInterfaceEnum.FollowRequestUpdateRegedit,new FollowRequestUpdateRegeditHandler());
                put(NettyInterfaceEnum.LeaderNotifyBrokerUpdateRegedit,new LeaderNotifyBrokerUpdateRegeditHandler());
                put(NettyInterfaceEnum.NotifyMasterVoteNewSlave,new NotifyMasterVoteNewSlaveHandler());
                put(NettyInterfaceEnum.NotifyLeaderUpdateRegeditForDataStatus,new NotifyLeaderUpdateRegeditForDataStatusHandler());
                put(NettyInterfaceEnum.LeaderNotifyFollwUpdateBakLeaderInfo,new LeaderNotifyFollwUpdateBakLeaderHandler());
                put(NettyInterfaceEnum.LeaderNotifySalveUpdateRegedit,new LeaderNotifySalveUpdateRegeditHandler());
                put(NettyInterfaceEnum.SalveRequestUpdateClusterRegedit,new ClusterSlaveRequestUpdateRegeditHandler());
                put(NettyInterfaceEnum.ClientUpdateBrokers,new ClientRequestUpdateBrokersHandler());
                put(NettyInterfaceEnum.BrokerUpdateClients,new BrokerRequestUpdateClientsHandler());
            }
        };
    }
    public abstract ByteString process(Dto.Frame frame) throws Exception;
}
