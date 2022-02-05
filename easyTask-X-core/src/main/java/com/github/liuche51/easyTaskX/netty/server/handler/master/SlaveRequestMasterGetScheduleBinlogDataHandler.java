package com.github.liuche51.easyTaskX.netty.server.handler.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.SlaveNode;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * master响应slave的请求ScheduleBinlog同步数据
 */
public class SlaveRequestMasterGetScheduleBinlogDataHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBodyBytes().toStringUtf8();
        Long lastIndex = Long.valueOf(body);
        SlaveNode slaveNode = MasterService.SLAVES.get(frame.getSource());
        if(slaveNode!=null){
            slaveNode.setCurrentBinlogIndex(lastIndex);
        }
        List<BinlogSchedule> binlogScheduleList = MasterService.getScheduleBinlogByIndex(lastIndex);
        return ByteString.copyFromUtf8(JSONObject.toJSONString(binlogScheduleList));
    }
}
