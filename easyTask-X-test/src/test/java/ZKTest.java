import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.VoteClusterLeader;
import com.github.liuche51.easyTaskX.dto.zk.ZKHost;
import com.github.liuche51.easyTaskX.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.util.Util;
import com.github.liuche51.easyTaskX.zk.ZKService;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class ZKTest {
    public ZKTest() {
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void registerLeader() {
        try {
            EasyTaskConfig config=new EasyTaskConfig();
            config.setZkAddress("127.0.0.1:2181");
            ClusterService.setConfig(config);
            ZKNode data = new ZKNode();
            data.setHost("127.0.0.1");
            data.setCreateTime(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            data.setLastHeartbeat(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            List<ZKHost> follows = new LinkedList<>();
            ZKHost follow1 = new ZKHost("127.0.0.2");
            ZKHost follow2 = new ZKHost("127.0.0.3");
            follows.add(follow1);
            follows.add(follow2);
            data.setFollows(follows);
            ZKService.registerLeader(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
   public void getChildrenByPath(){
       try {
           EasyTaskConfig config=new EasyTaskConfig();
           config.setZkAddress("127.0.0.1:2181");
           ClusterService.setConfig(config);
           List<String> list = ZKService.getChildrenByPath("/Server");
           list.forEach(x -> {
               System.out.println(x);
           });
       } catch (Exception e) {
           e.printStackTrace();
       }
   }
    @Test
    public void competeLeader() {
        EasyTaskConfig config=new EasyTaskConfig();
        config.setZkAddress("127.0.0.1:2181");
        ClusterService.setConfig(config);
        ClusterService.CURRENTNODE=new Node("127.0.0.1",2121);
        for(int i=0;i<5;i++){
            int name=i;
            Thread th=new Thread(new Runnable() {
                @Override
                public void run() {
                    VoteClusterLeader.competeLeader();
                }
            });
            th.start();
        }
        while (true){
            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
