package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveUtil;
import com.github.liuche51.easyTaskX.cluster.task.leader.ClusterMetaBinLogSyncTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class BakLeaderService {
    /**
     * 与leader元数据的数据一致性状态。0同步中，1已同步
     */
    public static Integer DATA_STATUS = 0;

    /**
     * 请求leader获取集群元数据
     *
     * @param leader
     * @param task   当前运行的任务
     * @throws Exception
     */
    public static void requestLeaderSyncClusterMetaData(BaseNode leader, ClusterMetaBinLogSyncTask task) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BakLeaderRequestLeaderGetClusterMetaBinlogData).setSource(NodeService.getConfig().getAddress())
                .setBody(String.valueOf(task.getCurrentIndex()));
        ByteStringPack respPack = new ByteStringPack();
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, leader.getClient(), NodeService.getConfig().getAdvanceConfig().getTryCount(), 5, respPack);
        if (ret) {
            List<BinlogClusterMeta> binlogClusterMetas = JSONObject.parseArray(respPack.getRespbody().toString(), BinlogClusterMeta.class);
            if (binlogClusterMetas == null || binlogClusterMetas.size() < NodeService.getConfig().getAdvanceConfig().getBinlogCount()) {
                BakLeaderService.DATA_STATUS = 1;
            } else if (binlogClusterMetas != null && binlogClusterMetas.size() >= NodeService.getConfig().getAdvanceConfig().getBinlogCount()) {
                BakLeaderService.DATA_STATUS = 0;
            }
            if (binlogClusterMetas == null || binlogClusterMetas.size() == 0) return;
            Collections.sort(binlogClusterMetas, Comparator.comparing(BinlogClusterMeta::getId));
            for (BinlogClusterMeta x : binlogClusterMetas) {
                Long id = SlaveUtil.saveBinlogClusterMeta(x);
                if (id != null) // id为null表示当前处理的是心跳类数据，不需要保存处理位置
                    task.setCurrentIndex(id);
            }
        }else {// 如果网络失败，就应该标记为同步中
            BakLeaderService.DATA_STATUS = 0;
        }
    }
}
