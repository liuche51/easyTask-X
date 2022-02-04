package com.github.liuche51.easyTaskX.cluster.task.broker;


import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.Util;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Broker从leader更新Client列表。
 * 低频率
 */
public class BrokerUpdateClientsTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
               Dto.Frame.Builder builder= Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.BrokerRequestLeaderSendClients).setSource(BrokerService.getConfig().getAddress());
                ByteStringPack pack=new ByteStringPack();
                boolean ret= NettyMsgService.sendSyncMsgWithCount(builder, BrokerService.CLUSTER_LEADER.getClient(),1,0,pack);
                if(ret){
                   StringListDto.StringList list=StringListDto.StringList.parseFrom(pack.getRespbody()) ;
                   List<String> brokers=list.getListList();
                   BrokerService.CLIENTS.clear();
                   if(brokers!=null){
                       brokers.forEach(x->{
                           BrokerService.CLIENTS.add(new BaseNode(x));
                       });
                   }
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                TimeUnit.HOURS.sleep(BrokerService.getConfig().getAdvanceConfig().getUpdateClientsTime());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }

}
