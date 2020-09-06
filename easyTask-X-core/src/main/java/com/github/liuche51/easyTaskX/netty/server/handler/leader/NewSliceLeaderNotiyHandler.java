package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.leader.SliceLeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

/**
 * 处理选举的新分片leader后续逻辑
 */
public class NewSliceLeaderNotiyHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body=frame.getBody();
        String[] items=body.split("|");
        if(ClusterService.CURRENTNODE.getAddress().equals(items[1])){
            SliceLeaderService.submitNewTaskByOldLeader(items[0]);
        }else {
            SliceLeaderService.deleteOldLeaderBackTask(items[0]);
        }
        ClusterService.updateRegedit();
        return null;
    }
}
