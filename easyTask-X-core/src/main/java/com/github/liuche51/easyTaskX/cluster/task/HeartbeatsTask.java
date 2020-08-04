package com.github.liuche51.easyTaskX.cluster.task;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
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
                ZKNode node = ZKService.getDataByCurrentNode();
                //防止节点信息已经被其他节点删除了。说明当前节点实际上已经失去了和zk的心跳。重新初始化集群
                //心跳超出死亡时间的也重新初始化集群
                if(node==null|| DateUtils.isGreaterThanDeadTime(node.getLastHeartbeat(),0)){
                    Thread th2=new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                log.info("restart cluster initCurrentNode()  ZKNode="+ JSONObject.toJSONString(node));
                                ClusterService.initCurrentNode();
                                log.info("finished restarted cluster");
                            }catch (Exception e){
                                log.error("restart cluster initCurrentNode() exception!",e);
                            }
                        }
                    });
                    th2.start();
                }else {
                    node.setLastHeartbeat(DateUtils.getCurrentDateTime());
                    node.setLeaders(Util.nodeToZKHost(ClusterService.CURRENTNODE.getLeaders()));//直接将本地数据覆盖到zk
                    node.setFollows(Util.nodeToZKHost(ClusterService.CURRENTNODE.getFollows()));//直接将本地数据覆盖到zk
                    boolean ret = ZKService.setDataByCurrentNode(node);
                    if (!ret) {//设置新值失败，说明zk注册信息已经被follow删除，follows已经重新选举出一个新leader。本节点只能重新选follows了
                       System.out.println("ZKService.setDataByCurrentNode 设置新值失败");
                        ZKService.register(new ZKNode(ClusterService.CURRENTNODE.getHost(), ClusterService.CURRENTNODE.getPort()));
                        LeaderService.initSelectFollows();
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
