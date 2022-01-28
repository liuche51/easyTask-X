package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 注册表用服务端节点对象
 */
public class RegClient extends RegNode {

    public RegClient(BaseNode baseNode) {
        super(baseNode.getHost(), baseNode.getPort());
    }

    public RegClient(RegNode regNode) {
        super(regNode.getHost(), regNode.getPort());
    }

    public RegClient(String host, int port) {
        super(host, port);
    }

    public RegClient(String address) {
        super(address);
    }
}
