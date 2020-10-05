package com.github.liuche51.easyTaskX.netty.server.handler.leader;

import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 * 息leader响应Broker请求更新Client列表信
 */
public class BrokerRequestUpdateClientsHandler extends BaseHandler {
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
