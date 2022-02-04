package com.github.liuche51.easyTaskX.cluster.task.master;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.master.MasterService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.MasterNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.enume.NodeStatusEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.*;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Master清理无用的数据定时任务
 * 1、
 */
public class ClearDataTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                clearScheduleBakData();
                clearSLAVE_SYNC_TASK_RECORD();
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                TimeUnit.HOURS.sleep(BrokerService.getConfig().getAdvanceConfig().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }

    /**
     * 清理任务备份库数据
     * 1、master已经失效了的情况下。
     * 2、且自身不是新Master。如果是，则需要等到提交到自身主任务库后，才能清理。不再此处操作
     */
    private void clearScheduleBakData() {
        try {
            if (!requestLeaderNodeStatusIsNormal()) return;//只有正常状态才能清理。
            Map<String, MasterNode> masters = SlaveService.MASTERS;
            Iterator<Map.Entry<String, MasterNode>> items = masters.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
            List<String> sources = new ArrayList<>(masters.size());
            while (items.hasNext()) {
                Map.Entry<String, MasterNode> item = items.next();
                sources.add(item.getValue().getAddress());
            }
            ScheduleBakDao.deleteNotInBySources(sources.toArray(new String[sources.size()]));
        } catch (Exception e) {
            LogUtil.error("", e);
        }
    }

    /**
     * 清理Master的master和slave同步提交任务状态记录超时数据
     * 1、防止日积月累。导致内存问题
     */
    private void clearSLAVE_SYNC_TASK_RECORD() {
        Iterator<Map.Entry<String, Map<String, Object>>> items = MasterService.SLAVE_SYNC_TASK_RECORD.entrySet().iterator();
        while (items.hasNext()) {
            Map.Entry<String, Map<String, Object>> item = items.next();
            long time = (long) item.getValue().get("time");
            if (DateUtils.getTimeStamp(ZonedDateTime.now().plusHours(1)) > time) {
                items.remove();
                MasterService.addWAIT_DELETE_TASK(item.getKey());
            }
        }
    }

    /**
     * 请求leader当前节点状态是否正常
     *
     * @return
     */
    public static boolean requestLeaderNodeStatusIsNormal() {
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowRequestLeaderGetRegNodeStatus).setSource(BrokerService.getConfig().getAddress())
                    .setBody(StringConstant.BROKER);
            ByteStringPack respPack = new ByteStringPack();
            boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, BrokerService.CLUSTER_LEADER.getClient(), BrokerService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
            if (ret) {
                String result = respPack.getRespbody().toStringUtf8();
                if (!StringUtils.isNullOrEmpty(result)) {
                    Map<String, Integer> map = JSONObject.parseObject(result, Map.class);
                    Integer value = map.get(StringConstant.NODESTATUS);
                    if (value.equals(NodeStatusEnum.NORMAL))
                        return true;
                }

            } else {
                LogUtil.info("normally exception!requestLeaderNodeStatusIsNormal() failed.");
            }
        } catch (Exception e) {
            LogUtil.error("", e);
        }
        return false;
    }
}
