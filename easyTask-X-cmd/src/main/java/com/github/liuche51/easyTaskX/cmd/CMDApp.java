package com.github.liuche51.easyTaskX.cmd;

import com.github.liuche51.easyTaskX.cmd.socket.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.Scanner;

/**
 * CMD客户端启动类
 */
public class CMDApp {
    private static Logger log = LoggerFactory.getLogger(CMDApp.class);
    private static final String CONNECT_HOST_ADDRESS = "connect host address:";
    private static final String CONNECT = "connect";
    private static final String EXIT = "exit";

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String lastCommand = "";
        String status = "";
        String address = "";
        Scanner sc = new Scanner(System.in);
        System.out.print(CONNECT_HOST_ADDRESS);
        status = CONNECT;//连接服务端状态
        while (true) {//反复保持与服务端通信
            try {
                String te = sc.nextLine();
                lastCommand = te.trim();
                //退出命令。
                if (EXIT.equalsIgnoreCase(te.trim())) {
                    Client.close();
                    System.out.print(CONNECT_HOST_ADDRESS);
                    status = CONNECT;
                }
                //连接命令。127.0.0.1:3031
                else if (CONNECT.equalsIgnoreCase(status)) {
                    String[] arry = lastCommand.split(":");
                    Client.connect(arry[0], Integer.parseInt(arry[1]));
                    status = "normally";//正常状态
                    address = lastCommand.trim();
                }
                //普通命令
                else {
                    String ret = Client.send(te);
                    System.out.println(ret);
                }
                if (!EXIT.equals(lastCommand))
                    System.out.print("[" + address + "]:");
            } catch (SocketException e) {
                System.out.println("服务端已断开连接...");
                dealException(status, lastCommand);
            } catch (Exception e) {
                e.printStackTrace();
                dealException(status, lastCommand);
            }

        }
    }

    private static void dealException(String status, String lastCommand) {
        Client.close();
        System.out.print(CONNECT_HOST_ADDRESS);
        status = CONNECT;
        lastCommand = EXIT;
    }
}
