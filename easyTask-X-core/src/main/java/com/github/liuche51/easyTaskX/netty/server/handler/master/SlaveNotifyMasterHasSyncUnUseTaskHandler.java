package com.github.liuche51.easyTaskX.netty.server.handler.master;

import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * slave通知Master，已经同步了还不能使用的任务。
 */
public class SlaveNotifyMasterHasSyncUnUseTaskHandler extends BaseHandler {

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        StringListDto.StringList list = StringListDto.StringList.parseFrom(frame.getBodyBytes());
        List<String> tasks = list.getListList();
        for (String task : tasks) {
            String[] split = task.split(StringConstant.CHAR_SPRIT_COMMA);//任务ID,状态,错误信息
            String taskId = split[0], error = split[2];
            int status = Integer.parseInt(split[1]);
            Map<String, Object> map = MasterService.SLAVE_SYNC_TASK_RECORD.get(taskId);
            if (map == null) {//第一个反馈的slave处理就行，后续反馈的就直接抛弃。避免重复处理
                continue;
            }
            MasterService.SLAVE_SYNC_TASK_RECORD.remove(taskId);
            if (status == 1) { // 如果成功，则放入反馈成功队列。等待Master进一步处理（单线程更新任务状态）。这里不直接处理更新状态，是担心多线程并发处理导致数据库繁忙。影响性能
                MasterService.SLAVE_RESPONSE_SUCCESS_TASK_RESULT.put(new SubmitTaskResult(taskId, SubmitTaskResultStatusEnum.SUCCESSED, StringConstant.EMPTY, (String) map.get("source")));
            } else if (status == 9) { // 如果处理失败，则直接放入反馈客户端队列
                MasterService.addWAIT_RESPONSE_CLINET_TASK_RESULT((String) map.get("source"), new SubmitTaskResult(taskId, SubmitTaskResultStatusEnum.FAILED, error));
            }
        }
        return null;
    }
}