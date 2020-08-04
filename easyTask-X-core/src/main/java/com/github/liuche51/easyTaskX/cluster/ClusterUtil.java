package com.github.liuche51.easyTaskX.cluster;

import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

public class ClusterUtil {
    private static final Logger log = LoggerFactory.getLogger(ClusterUtil.class);

    /**
     * 带重试次数的同步消息发送
     *
     * @param client
     * @param msg
     * @param tryCount
     * @return
     */
    public static boolean sendSyncMsgWithCount(NettyClient client, Object msg, int tryCount) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        Object ret=null;
        Dto.Frame frame=null;
        try {
            ret =  NettyMsgService.sendSyncMsg(client,msg);
            frame = (Dto.Frame) ret;
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult()))
                return true;
            else
                error = result.getMsg();
        }
        catch (Exception e) {
            log.error("sendSyncMsgWithCount exception!error=" + error, e);
        }
        finally {
            tryCount--;
        }
        log.info("sendSyncMsgWithCount error" + error + ",tryCount=" + tryCount + ",objectHost=" + client.getObjectAddress());
        return sendSyncMsgWithCount(client, msg, tryCount);

    }
    /**
     * 同步与目标主机的时间差
     * @param node
     * @return
     */
    public static boolean syncObjectNodeClockDiffer(Node node, int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        Dto.Frame frame = null;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName(NettyInterfaceEnum.SYNC_CLOCK_DIFFER).setSource(ClusterService.getConfig().getAddress())
                    .setBody(ClusterService.getConfig().getAddress());
            long start = System.currentTimeMillis();
            frame = NettyMsgService.sendSyncMsg(node.getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                long nodetime = Long.parseLong(result.getBody());
                long end=System.currentTimeMillis();
                long differ=end-nodetime-(end-start)/2;
                long second=differ/1000;
                log.info("Object host["+node.getAddress()+"] clock differ "+second+"s.");
                node.getClockDiffer().setDifferSecond(second);
                node.getClockDiffer().setHasSync(true);
                node.getClockDiffer().setLastSyncDate(ZonedDateTime.now());
                return true;
            }else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("syncObjectNodeClockDiffer() exception!", e);
        }finally {
            tryCount--;
        }
        log.info("syncObjectNodeClockDiffer()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + node.getAddress());
        try {
            Thread.sleep(waiteSecond*1000);
        } catch (InterruptedException e) {
            log.error("",e);
        }
        return syncObjectNodeClockDiffer(node, tryCount,waiteSecond);
    }
}
