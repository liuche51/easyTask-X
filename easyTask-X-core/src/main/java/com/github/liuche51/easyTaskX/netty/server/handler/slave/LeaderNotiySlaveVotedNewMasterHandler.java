package com.github.liuche51.easyTaskX.netty.server.handler.slave;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.task.master.NewMasterSubmitBakTask;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * slave响应：leader通知slaves，已经选举了新master
 */
public class LeaderNotiySlaveVotedNewMasterHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split(StringConstant.CHAR_SPRIT_STRING);//失效master，新master
        //如果自己就是新master。就重新提交旧master的任务给自己
        if(BrokerService.CURRENT_NODE.getAddress().equals(items[1])){
            String oldMasterAddress=items[0];
            NewMasterSubmitBakTask task = new NewMasterSubmitBakTask(oldMasterAddress);
            task.start();
            BrokerService.onceTasks.add(task);
        }
        //如果自己不是新master。则直接删除旧master的备份数据。新Master删除备份数据逻辑放到上面的异步任务里去
        else {
            MasterService.deleteOldMasterBackTask(items[0]);
        }
        return null;
    }
}
