package com.github.liuche51.easyTaskX.netty.server.handler.notify;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.leader.ClusterLeaderService;
import com.github.liuche51.easyTaskX.cluster.leader.SliceLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * 分片follow响应：集群leader通知分片follows，已经选举了新分片leader
 */
public class NotiyNewSliceLeaderHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split("|");
        ClusterLeaderService.requestUpdateRegedit();
        //如果自己就是新leader。就重新提交旧leader的任务给自己
        if(ClusterService.CURRENTNODE.getAddress().equals(items[1])){
            SliceLeaderService.submitNewTaskByOldLeader(items[0]);
        }
        //如果自己不是新leader。则删除旧leader的备份数据
        else {
            SliceLeaderService.deleteOldLeaderBackTask(items[0]);
        }
        return null;
    }
}
