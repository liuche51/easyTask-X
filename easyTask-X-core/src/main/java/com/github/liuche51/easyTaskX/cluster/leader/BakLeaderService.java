package com.github.liuche51.easyTaskX.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.slave.SlaveService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.cluster.task.slave.BakLeaderRequestUpdateRegeditTask;
import com.github.liuche51.easyTaskX.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.dto.RegBroker;
import com.github.liuche51.easyTaskX.dto.RegClient;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class BakLeaderService {
    private static final Logger log = LoggerFactory.getLogger(BakLeaderService.class);

    /**
     * 启动BakLeader主动通过定时任务从leader更新注册表
     */
    public static TimerTask startBakLeaderRequestUpdateRegeditTask() {
        BakLeaderRequestUpdateRegeditTask task = new BakLeaderRequestUpdateRegeditTask();
        task.start();
        NodeService.timerTasks.add(task);
        return task;
    }
}
