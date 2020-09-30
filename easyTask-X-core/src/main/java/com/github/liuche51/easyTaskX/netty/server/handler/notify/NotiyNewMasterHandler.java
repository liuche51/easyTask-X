package com.github.liuche51.easyTaskX.netty.server.handler.notify;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * slave响应：leader通知slaves，已经选举了新master
 */
public class NotiyNewMasterHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split("|");
        //如果自己就是新leader。就重新提交旧leader的任务给自己
        if(ClusterService.CURRENTNODE.getAddress().equals(items[1])){
            MasterService.submitNewTaskByOldLeader(items[0]);
        }
        //如果自己不是新leader。则删除旧leader的备份数据
        else {
            MasterService.deleteOldLeaderBackTask(items[0]);
        }
        return null;
    }
}
