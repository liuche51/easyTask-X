package com.github.liuche51.easyTaskX.netty.server.handler;


import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseHandler {
    public static Map<String,BaseHandler> INSTANCES;
    static {
        INSTANCES=new HashMap<String,BaseHandler>(){
            {
                put(NettyInterfaceEnum.CLIENT_SUBMIT_TASK,new ClientSubmitTaskHandler());
                put(NettyInterfaceEnum.TRAN_TRYSAVETASK,new TranTrySaveTaskHandler());
                put(NettyInterfaceEnum.TRAN_CONFIRMSAVETASK,new TranConfirmSaveTaskHandler());
                put(NettyInterfaceEnum.TRAN_CANCELSAVETASK,new TranCancelSaveTaskHandler());
                put(NettyInterfaceEnum.TRAN_TRYDELTASK,new TranTryDelTaskHandler());
                put(NettyInterfaceEnum.LEADER_SYNC_DATA_TO_NEW_FOLLOW,new LeaderSyncDataToNewFollowHandler());
                put(NettyInterfaceEnum.SYNC_LEADER_POSITION,new SyncLeaderPositionHandler());
                put(NettyInterfaceEnum.GET_DBINFO_BY_TASKID,new GetDBInfoByTaskIdHandler());
            }
        };
    }
    public abstract String process(Dto.Frame frame) throws Exception;
}
