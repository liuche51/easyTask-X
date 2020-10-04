package com.github.liuche51.easyTaskX.dto.zk;

import com.github.liuche51.easyTaskX.cluster.NodeService;

public class LeaderData {
    private String host;
    private int port= NodeService.getConfig().getServerPort();
    /**
     * 最近一次心跳时间
     */
    private String lastHeartbeat;
    private String createTime;
    public LeaderData(String host,int port){
        this.host=host;
        this.port=port;
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

    public String getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(String lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
