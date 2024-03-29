package com.github.liuche51.easyTaskX.socket;

import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.util.LogUtil;

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

    public static void init() {
        Thread task = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    start();
                } catch (IOException e) {
                    LogUtil.error("init()-> exception!", e);
                }
            }
        });
        task.start();
    }

    private static void start() throws IOException {
        ServerSocket server = new ServerSocket(BrokerService.getConfig().getCmdPort());//启动端口提供服务
        LogUtil.info("CmdServer started! on port {}", BrokerService.getConfig().getCmdPort());
        while (true) {//循环监听客户端。如果当前客户端断开连接，则进入下一次客户连接等待
            Socket client = null;
            try {
                client = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream());
                while (true) {//循环监听当前客户端的发送消息
                    String cmd = in.readLine();
                    //判断客户端是否已经关闭
                    if(isClientClose(client))
                    {
                        client.close();
                        break;
                    }
                    LogUtil.info("received cmd client cmd:{}", cmd);
                    Command cm = CmdEngine.parse(cmd);
                    if (cm == null) {
                        out.println("invalid command!" + cmd);
                    } else {
                        String ret = CmdService.excuteCmd(cm);
                        out.println(ret);
                    }
                    out.flush();
                }
            } catch (Exception e) {
                LogUtil.error("CMD Session exception!", e);
            } finally {
                if (client != null)
                    client.close();
            }

        }

    }
    /**
     * 判断是否断开连接，断开返回true,没有返回false
     * @param socket
     * @return
     */
    public static boolean isClientClose(Socket socket){
        try{
            socket.sendUrgentData(0xFF);//发送1个字节的紧急数据，默认情况下，服务器端没有开启紧急数据处理，不影响正常通信
            return false;
        }catch(Exception se){
            return true;
        }
    }
}
