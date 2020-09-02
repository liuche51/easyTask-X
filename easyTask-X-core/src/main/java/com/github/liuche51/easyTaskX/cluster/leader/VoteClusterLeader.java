package com.github.liuche51.easyTaskX.cluster.leader;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.util.StringUtils;
import com.github.liuche51.easyTaskX.zk.ZKService;
import com.github.liuche51.easyTaskX.zk.ZKUtil;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 选举集群leader
 * 采用分布式锁的方式实现
 */
public class VoteClusterLeader {
    private static Logger log = LoggerFactory.getLogger(VoteClusterLeader.class);
    private static InterProcessMutex zkMutex = new InterProcessMutex( ZKUtil.getClient(),"/mutex");
    public static boolean competeLeader(){
        boolean hasLock=false;
        try {
            if(zkMutex.acquire(1, TimeUnit.SECONDS)){
                hasLock=true;
                LeaderData data=ZKService.getClusterLeaderData();
                if(data==null&& !StringUtils.isNullOrEmpty(data.getHost())){//leader节点为空时才需要选新leader
                    ZKService.registerLeader(new ZKNode(ClusterService.CURRENTNODE.getHost(),ClusterService.CURRENTNODE.getPort()));
                    return true;
                }

            }
        } catch (Exception e) {
           log.error("competeLeader()->exception!",e);
        }
        finally {
            if(hasLock){
                try {
                    zkMutex.release();//释放锁，会删除mutex节点下的子节点（参与竞争的节点信息），所以你可能看不到，因为存续时间非常短
                } catch (Exception e) {
                    log.error("zkMutex.release()->exception!",e);
                }
            }

        }
        return false;
    }
}
