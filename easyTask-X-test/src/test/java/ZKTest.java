import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.VoteClusterLeader;
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
