package com.github.liuche51.easyTaskX.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * CMD客户端的服务端
 */
public class CmdServer {
    private static Logger log = LoggerFactory.getLogger(CmdServer.class);

    public static void start() throws IOException {
        ServerSocket server = new ServerSocket(5678);//启动端口提供服务
        log.info("CmdServer started!");
        while (true) {//循环监听客户端。如果当前客户端断开连接，则进入下一次客户连接等待
            try {
                Socket client = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream());
                while (true) {//循环监听当前客户端的发送消息
                    String str = in.readLine();
                    log.info("received cmd client msg:{}", str);
                    out.println("server say：" + str);
                    out.flush();
                }
            } catch (Exception e) {
                log.error("会话异常!", e);
            }
        }

    }
}
