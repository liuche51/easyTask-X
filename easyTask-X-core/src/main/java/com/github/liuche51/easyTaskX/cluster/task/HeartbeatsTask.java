package com.github.liuche51.easyTaskX.cluster.task;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.LeaderService;

import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.util.DateUtils;
import com.github.liuche51.easyTaskX.zk.ZKService;
/**
 * 节点对zk的心跳。2s一次
 */
public class HeartbeatsTask extends TimerTask{
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Node leader=ClusterService.CURRENTNODE.getClusterLeader();
                if(leader==null){
                    ZKNode node=ZKService.getClusterLeaderData();
                    if(node!=null){

                    }
                }


            } catch (Exception e) {
                log.error("",e);
            }
            try {
                Thread.sleep(ClusterService.getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("",e);
            }
        }
    }

}
