package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class ClusterLeaderUtil {
    private static final Logger log = LoggerFactory.getLogger(VoteSliceFollows.class);

    /**
     *
     * @param node
     * @param tryCount
     * @param waiteSecond
     * @return
     */
    public static boolean notifyNodeUpdateRegedit(Node node,int tryCount, int waiteSecond){
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.UPDATE_REGEDIT).setSource(ClusterService.getConfig().getAddress());
            Dto.Frame frame = NettyMsgService.sendSyncMsg(ClusterService.CURRENTNODE.getClusterLeader().getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("updateRegedit.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        log.info("updateRegedit()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + ClusterService.CURRENTNODE.getClusterLeader().getAddress());
        try {
            Thread.sleep(waiteSecond * 1000);
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return notifyNodeUpdateRegedit(node,tryCount, waiteSecond);
    }
}
