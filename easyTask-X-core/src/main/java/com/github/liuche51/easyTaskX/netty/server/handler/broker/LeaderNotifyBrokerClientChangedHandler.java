package com.github.liuche51.easyTaskX.netty.server.handler.broker;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dao.ScheduleDao;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTaskX.enume.OperationTypeEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.google.protobuf.ByteString;

import java.util.Iterator;

/**
 * Broker响应：leader通知brokers。Client已经变动
 */
public class LeaderNotifyBrokerClientChangedHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        String body = frame.getBody();
        String[] items = body.split(StringConstant.CHAR_SPRIT_STRING);//type+地址
        switch (items[0]) {
            case OperationTypeEnum.ADD:
                NodeService.CURRENTNODE.getClients().add(new BaseNode(items[1]));
                break;
            case OperationTypeEnum.DELETE:
                Iterator<BaseNode> temps = NodeService.CURRENTNODE.getClients().iterator();
                while (temps.hasNext()) {
                    BaseNode bn = temps.next();
                    if (bn.getAddress().equals(items[1])) {
                        NodeService.CURRENTNODE.getClients().remove(bn);
                        if (ScheduleDao.isExistByExecuter(bn.getAddress())) {
                            BrokerService.notifyLeaderUpdateRegeditForBrokerReDispatchTaskStatus(NodeSyncDataStatusEnum.SYNCING);
                            BrokerService.startReDispatchToClientTask(bn);
                        }
                    }
                }
                break;
            default:
                break;
        }
        return null;
    }
}
