package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class BaseNode implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(BaseNode.class);
    private String host = "";
    private int port;
    public BaseNode(String host,int port){
        this.host=host;
        this.port=port;
    }
    public BaseNode(String address) {
        String[] ret=address.split(":");
        this.host = ret[0];
        this.port = Integer.parseInt(ret[1]);
    }
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @JSONField(serialize = false)
    public String getAddress() {
        StringBuffer str = new StringBuffer(this.host);
        str.append(":").append(this.port);
        return str.toString();
    }
    @JSONField(serialize = false)
    public NettyClient getClient() throws InterruptedException {
        return NettyConnectionFactory.getInstance().getConnection(host, port);
    }

    /**
     * 获取Netty客户端连接。带重试次数
     * 目前一次通信，占用一个Netty连接。
     * @param tryCount
     * @return
     */
    @JSONField(serialize = false)
    public NettyClient getClientWithCount(int tryCount) throws Exception {
        if (tryCount == 0) throw new Exception("getClientWithCount()-> exception!");
        try {
            return getClient();
        } catch (Exception e) {
            log.info("getClientWithCount tryCount=" + tryCount + ",objectHost="+this.getAddress());
            log.error("getClientWithCount()-> exception!", e);
            return getClientWithCount(tryCount);
        } finally {
            tryCount--;
        }
    }
}
