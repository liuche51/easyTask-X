package com.github.liuche51.easyTaskX.netty.server.handler.broker;


import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Broker响应：Client删除任务处理类
 * 1、任务先入删除队列。异步删除
 */
public class ClientNotifyBrokerDeleteTaskHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        StringListDto.StringList list = StringListDto.StringList.parseFrom(frame.getBodyBytes());
        List<String> tasks = list.getListList();
        tasks.forEach(x -> {
            MasterService.addWAIT_DELETE_TASK(x);
        });
        return null;
    }
}
