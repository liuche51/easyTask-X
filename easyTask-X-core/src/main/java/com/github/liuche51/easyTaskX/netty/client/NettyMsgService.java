package com.github.liuche51.easyTaskX.netty.client;


import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.LogErrorDao;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.db.LogError;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.enume.LogErrorTypeEnum;
import com.github.liuche51.easyTaskX.util.LogUtil;
import com.github.liuche51.easyTaskX.util.StringConstant;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Netty客户端通信服务
 */
public class NettyMsgService {

    /**
     * 发送同步消息
     *
     * @param msg
     * @return
     */
    public static Dto.Frame sendSyncMsg(NettyClient conn, Object msg) throws InterruptedException {
        sendMsgPrintLog(conn, msg);
        ChannelPromise promise = conn.getClientChannel().newPromise();
        conn.getHandler().setPromise(promise);
        conn.getClientChannel().writeAndFlush(msg);
        promise.await(NodeService.getConfig().getAdvanceConfig().getTimeOut(), TimeUnit.SECONDS);//等待固定的时间，超时就认为失败，需要重发
        try {
            Dto.Frame frame = (Dto.Frame) conn.getHandler().getResponse();
            return frame;
        } finally {
            NettyConnectionFactory.getInstance().releaseConnection(conn);//目前是一次通信一次连接，所以需要通信完成后释放连接资源
        }
    }

    /**
     * 发送异步消息。不通过信号量控制。
     * 可以实现一个Nettyclient并发处理N个请求。但不能使用future.addListener方式处理返回结果了。
     * 需要在com.github.liuche51.easyTask.backup.client.ClientHandler#channelRead0中统一处理。
     * 这样需要每个请求中附带一个唯一标识。服务端返回结果时也戴上这个标识才行。否则就不知道处理的是哪个请求返回的结果。
     *
     * @param msg
     * @return
     */
    public static ChannelFuture sendASyncMsg(NettyClient conn, Object msg) {
        sendMsgPrintLog(conn, msg);
        return conn.getClientChannel().writeAndFlush(msg);
    }

    private static void sendMsgPrintLog(NettyClient conn, Object msg) {
        StringBuilder str = new StringBuilder("Client send to:");
        str.append(conn.getObjectAddress()).append(" msg : ").append(msg);
        LogUtil.debug(str.toString());
    }

    /**
     * 同步发送消息。使用重试次数
     *
     * @param builder
     * @param client
     * @param tryCount
     * @param waiteSecond
     * @return
     */
    public static boolean sendSyncMsgWithCount(Dto.Frame.Builder builder, NettyClient client, int tryCount, int waiteSecond, ByteStringPack respPack) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame frame = sendSyncMsg(client, builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                if (respPack != null)
                    respPack.setRespbody(result.getBodyBytes());
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            LogUtil.error("sendSyncMsgWithCount.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        LogUtil.info("normally exception!sendSyncMsgWithCount()-> error=" + error + ",tryCount=" + tryCount + ",objectHost=" + client.getObjectAddress());
        try {
            TimeUnit.SECONDS.sleep(waiteSecond);
        } catch (InterruptedException e) {
            LogUtil.error("", e);
        }
        return sendSyncMsgWithCount(builder, client, tryCount, waiteSecond, respPack);
    }


}
