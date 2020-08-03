package com.github.liuche51.easyTaskX.netty.server;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.follow.FollowService;

import com.github.liuche51.easyTaskX.monitor.DBMonitor;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.server.handler.BaseHandler;
import com.github.liuche51.easyTaskX.util.StringConstant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ServerHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(ServerHandler.class);

    /**
     * 接受客户端发过来的消息。
     *
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        // 收到消息直接打印输出
		StringBuilder str=new StringBuilder("Received Client:");
		str.append(ctx.channel().remoteAddress()).append( " send : ").append(msg);
        log.debug(str.toString());
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        ResultDto.Result.Builder result = ResultDto.Result.newBuilder();
        result.setResult(StringConstant.TRUE);
        builder.setInterfaceName(StringConstant.TRUE);
        try {
            builder.setSource(ClusterService.getConfig().getAddress());
            Dto.Frame frame = (Dto.Frame) msg;
            builder.setIdentity(frame.getIdentity());
            BaseHandler handler=BaseHandler.INSTANCES.get(frame.getInterfaceName());
            if(handler==null) throw new Exception("unknown interface method!");
            result.setBody(handler.process(frame));
        } catch (Exception e) {
            log.error("Deal client msg occured error！", e);
            result.setResult(StringConstant.FALSE);
        }
        builder.setBodyBytes(result.build().toByteString());
        ctx.writeAndFlush(builder.build());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("RamoteAddress : " + ctx.channel().remoteAddress() + " active !");
        try {
            ctx.writeAndFlush("Welcome to " + InetAddress.getLocalHost().getHostName() + " service!\n");
            super.channelActive(ctx);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            log.error("channelActive exception!", e);
        }

    }
}