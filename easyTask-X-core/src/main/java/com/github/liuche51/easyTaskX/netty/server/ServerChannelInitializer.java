package com.github.liuche51.easyTaskX.netty.server;

import com.github.liuche51.easyTaskX.dto.proto.Dto;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

/**
 *
 */
public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel socketChannel) {
    	 // 解码编码
        // 半包的处理
        socketChannel.pipeline().addLast(new ProtobufVarint32FrameDecoder());
        // 需要解码的目标类
        socketChannel.pipeline().addLast(new ProtobufDecoder(Dto.Frame.getDefaultInstance()));
        socketChannel.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
        socketChannel.pipeline().addLast(new ProtobufEncoder());

        // 自己的逻辑Handler
        // 配置入站、出站事件channel 
        socketChannel.pipeline().addLast(new ServerHandler());
    }
}