package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 * leader响应:Client通过定时任务请求leader更新Broker列表信息
 */
public class ClientRequestLeaderSendBrokersHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        StringListDto.StringList.Builder builder=StringListDto.StringList.newBuilder();
        Iterator<String> items =LeaderService.BROKER_REGISTER_CENTER.keySet().iterator();
        while (items.hasNext()){
            builder.addList(items.next());
        }
        return builder.build().toByteString();
    }
}
