package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.RegNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderUtil {
    private static final Logger log = LoggerFactory.getLogger(VoteSlave.class);

    /**
     * 通知节点更新注册表信息
     * @param node
     */
    public static void notifyNodeUpdateRegedit(RegNode node){
        ClusterService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.NotifyNodeUpdateRegedit)
                            .setSource(ClusterService.CURRENTNODE.getAddress());
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, node.getClient(), ClusterService.getConfig().getAdvanceConfig().getTryCount(), 5, null);
                    if (!ret)
                        log.info("normally exception!notifyNodeUpdateRegedit() failed.");
                } catch (Exception e) {
                    log.error("notifyNodeUpdateRegedit()->exception!", e);
                }
            }
        });
    }
}
