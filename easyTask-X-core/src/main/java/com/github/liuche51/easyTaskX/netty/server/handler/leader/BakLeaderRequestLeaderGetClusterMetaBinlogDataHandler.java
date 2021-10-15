package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * leader响应bakleader集群元数据Binlog
 * 1、首先优先获取注册表增删改主要操作日志数据。从库中
 * 2、如果没有binlog数据就获取bakleader的心跳数据发送队列里的。
 */
public class BakLeaderRequestLeaderGetClusterMetaBinlogDataHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBodyBytes().toStringUtf8();
        List<BinlogClusterMeta> binlogClusterMetas = LeaderService.getBinlogClusterMetaByIndex(Long.valueOf(body));
        if(binlogClusterMetas==null||binlogClusterMetas.size()==0){
            LinkedBlockingQueue<String> bakleaderMetaQueue = LeaderService.followsHeartbeats.get(frame.getSource());
            if(bakleaderMetaQueue!=null){
                binlogClusterMetas=new LinkedList<>();
                for(int i = 0; i<NodeService.getConfig().getAdvanceConfig().getBinlogCount();i++){
                    String element = bakleaderMetaQueue.poll();
                    if(!StringUtils.isNullOrEmpty(element)){
                        String[] ret=element.split(StringConstant.CHAR_SPRIT_COMMA);// regnode,address,time
                        binlogClusterMetas.add(new BinlogClusterMeta(StringConstant.UPDATE_HEARTBEATS,ret[0],ret[1],ret[2]));
                    }
                }
            }
        }
        return ByteString.copyFromUtf8(JSONObject.toJSONString(binlogClusterMetas));
    }
}