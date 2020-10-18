package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 * leader响应:Broker通过定时任务请求leader发送Client列表信息给Broker
 */
public class BrokerRequestLeaderSendClientsHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        StringListDto.StringList.Builder builder=StringListDto.StringList.newBuilder();
        Iterator<String> items =LeaderService.CLIENT_REGISTER_CENTER.keySet().iterator();
        while (items.hasNext()){
            builder.addList(items.next());
        }
        return builder.build().toByteString();
    }
}
