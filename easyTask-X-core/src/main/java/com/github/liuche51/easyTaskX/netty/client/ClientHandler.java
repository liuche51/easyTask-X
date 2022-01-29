package com.github.liuche51.easyTaskX.netty.client;

import com.github.liuche51.easyTaskX.util.LogUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 客户端入站(收到服务端消息)事件监听。
 */
public class ClientHandler extends SimpleChannelInboundHandler<Object> {
    private ChannelHandlerContext ctx;
    /**
     * 线程同步信号量。用于客户端同步调用或异步调用需返回结果处理时
     */
    private ChannelPromise promise;
    /**
     * 同步调用时。返回服务端结果
     */
	private Object response;
    public void setPromise(ChannelPromise promise){
        this.promise=promise;
    }

    public Object getResponse() {
        return response;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        // 收到消息直接打印输出
        LogUtil.debug("Received Server:" + ctx.channel().remoteAddress() + " send : " + msg);
        //同步通信才会用到promise，异步不需要
        if (promise != null)
		{
			this.response=msg;
            promise.setSuccess();
		}else {

        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Client actived! ");
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Client Inactived! ");
        super.channelInactive(ctx);
    }

}