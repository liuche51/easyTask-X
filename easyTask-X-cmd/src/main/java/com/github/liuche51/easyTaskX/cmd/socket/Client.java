package com.github.liuche51.easyTaskX.cmd.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Client {
    private static Socket server;

    public static void connect(String host, int port) throws IOException {
        if (server == null)
            server = new Socket(host, port);
    }
    public static void close(){
        if(server!=null){
            try {
                server.close();
                server=null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static String send(String msg) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));
        PrintWriter out = new PrintWriter(server.getOutputStream());
        out.println(msg);
        out.flush();
        while (true) {
            String info = in.readLine();
            if (info != null)
                return info;
        }
    }
}
