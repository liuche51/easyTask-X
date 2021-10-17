package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

/**
 * leader响应：master通知leader，变更slave与master数据同步状态
 * 1、slave通过binlog机制异步同步master任务，当master查询到的本次请求binlog数量等于批量大小时，就可以认为该slave同步有延迟。及时上报给leader
 */
public class MasterNotifyLeaderChangeSlaveDataStatusHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) {
        String body=frame.getBody();
        String[] items=body.split(StringConstant.CHAR_SPRIT_STRING);//slave|状态
        RegBroker master= LeaderService.BROKER_REGISTER_CENTER.get(frame.getSource());
        RegNode slave=master.getSlaves().get(items[0]);
        slave.setDataStatus(new Integer(items[1]));
        return null;
    }
}
