package com.github.liuche51.easyTaskX.cluster.slave;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.db.BinlogClusterMeta;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.enume.RegNodeTypeEnum;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.util.StringConstant;
import netscape.javascript.JSObject;

/**
 * Follow类
 */
public class SlaveUtil {
    /**
     * 解析元数据对象，并存入bakleader本地集群元数据变量中
     *
     * @param meta
     * @return 当前元数据ID
     */
    public static Long saveBinlogClusterMeta(BinlogClusterMeta meta) {
        switch (meta.getOptype()) {
            case OperationTypeEnum.ADD:
            case OperationTypeEnum.UPDATE:
                if (meta.getRegnode().equals(RegNodeTypeEnum.REGBROKER)) {
                    LeaderService.BROKER_REGISTER_CENTER.put(meta.getKey(), JSONObject.parseObject(meta.getValue(), RegBroker.class));
                } else if (meta.getRegnode().equals(RegNodeTypeEnum.REGCLIENT)) {
                    LeaderService.CLIENT_REGISTER_CENTER.put(meta.getKey(), JSONObject.parseObject(meta.getValue(), RegClient.class));
                }
                break;
            case OperationTypeEnum.DELETE:
                if (meta.getRegnode().equals(RegNodeTypeEnum.REGBROKER)) {
                    LeaderService.BROKER_REGISTER_CENTER.remove(meta.getKey());
                } else if (meta.getRegnode().equals(RegNodeTypeEnum.REGCLIENT)) {
                    LeaderService.CLIENT_REGISTER_CENTER.remove(meta.getKey());
                }
            case StringConstant
                    .UPDATE_HEARTBEATS: // leader发现如果没有元数据binlog数据返回，就获取节点心跳队列里的数据返回给bakleader。复用数据传输对象
                if (meta.getRegnode().equals(RegNodeTypeEnum.REGBROKER)) {
                    RegBroker regBroker = LeaderService.BROKER_REGISTER_CENTER.get(meta.getKey());
                    if (regBroker != null)
                        regBroker.setLastHeartbeat(DateUtils.parse(meta.getValue()));
                } else if (meta.getRegnode().equals(RegNodeTypeEnum.REGCLIENT)) {
                    RegClient regClient = LeaderService.CLIENT_REGISTER_CENTER.get(meta.getKey());
                    if (regClient != null)
                        regClient.setLastHeartbeat(DateUtils.parse(meta.getValue()));
                }
                break;
            default:
                break;
        }
        return meta.getId();
    }
}
