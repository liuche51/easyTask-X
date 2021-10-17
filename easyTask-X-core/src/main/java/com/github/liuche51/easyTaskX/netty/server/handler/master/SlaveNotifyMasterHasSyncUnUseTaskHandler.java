package com.github.liuche51.easyTaskX.netty.server.handler.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * master响应。slave通知Master，已经同步了还不能使用的任务。
 */
public class SlaveNotifyMasterHasSyncUnUseTaskHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBodyBytes().toStringUtf8();
        Map<String, Object> values=new HashMap<>();
        values.put("status",1);
        ScheduleDao.updateByIds(new String[]{body},values);
        MasterService.TASK_SYNC_SALVE_STATUS.put(body, Boolean.TRUE);
        return null;
    }
}