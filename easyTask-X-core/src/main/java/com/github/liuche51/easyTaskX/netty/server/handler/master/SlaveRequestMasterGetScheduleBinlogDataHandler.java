package com.github.liuche51.easyTaskX.netty.server.handler.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerUtil;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.DataStatusEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * master响应slave的请求ScheduleBinlog同步数据
 */
public class SlaveRequestMasterGetScheduleBinlogDataHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBodyBytes().toStringUtf8();
        List<BinlogSchedule> binlogScheduleList = MasterService.getScheduleBinlogByIndex(Long.valueOf(body));
        if (binlogScheduleList.size() >= BrokerService.getConfig().getAdvanceConfig().getBinlogCount()) {//如果本次binlog数量等于配置的批量值，说明没有及时同步，上报leader。状态改为同步中
            Map<String, Integer> map = new HashMap<>(Util.getMapInitCapacity(1));
            map.put(StringConstant.DATASTATUS, DataStatusEnum.UNSYNC);
            BrokerUtil.notifyLeaderChangeRegNodeStatus(map);
        } else {
            Map<String, Integer> map = new HashMap<>(Util.getMapInitCapacity(1));
            map.put(StringConstant.DATASTATUS, DataStatusEnum.NORMAL);
        }
        return ByteString.copyFromUtf8(JSONObject.toJSONString(binlogScheduleList));
    }
}
