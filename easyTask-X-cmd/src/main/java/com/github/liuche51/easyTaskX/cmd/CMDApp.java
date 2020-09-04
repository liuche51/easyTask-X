package com.github.liuche51.easyTaskX.cmd;

import com.github.liuche51.easyTaskX.cmd.socket.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Scanner;

/**
 * CMD客户端启动类
 */
public class CMDApp {
    private static Logger log = LoggerFactory.getLogger(CMDApp.class);

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.print("connect host address:");
        String address ="127.0.0.1:3031";// sc.nextLine();//127.0.0.1:3031
        String[] arry=address.split(":");
        Client.connect(arry[0],Integer.parseInt(arry[1]));
       while (true){//反复保持与服务端通信
           System.out.print("["+address+"]:");
           String te = sc.nextLine();
           String ret=Client.send(te);
           System.out.println(ret);
       }
    }
}
