import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.dto.Node;
import com.github.liuche51.easyTaskX.cluster.leader.VoteLeader;
import com.github.liuche51.easyTaskX.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.zk.ZKService;
import org.junit.Test;

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
            NodeService.setConfig(config);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
   public void getChildrenByPath(){
       try {
           EasyTaskConfig config=new EasyTaskConfig();
           config.setZkAddress("127.0.0.1:2181");
           NodeService.setConfig(config);
           /*List<String> list = ZKService.getChildrenByPath("/Server");
           list.forEach(x -> {
               System.out.println(x);
           });*/
       } catch (Exception e) {
           e.printStackTrace();
       }
   }
    @Test
    public void competeLeader() {
        EasyTaskConfig config=new EasyTaskConfig();
        config.setZkAddress("127.0.0.1:2181");
        NodeService.setConfig(config);
        NodeService.CURRENTNODE=new Node("127.0.0.1",2121);
        for(int i=0;i<5;i++){
            int name=i;
            Thread th=new Thread(new Runnable() {
                @Override
                public void run() {
                    VoteLeader.competeLeader();
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
    @Test
    public void watcherTest(){
        EasyTaskConfig config=new EasyTaskConfig();
        config.setZkAddress("127.0.0.1:2181");
        NodeService.setConfig(config);
        try {
            LeaderData d=ZKService.getLeaderData(true);
            int y=0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (true){
            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    @Test
    public void watcherTest2(){
        EasyTaskConfig config=new EasyTaskConfig();
        config.setZkAddress("127.0.0.1:2181");
        NodeService.setConfig(config);
        NodeService.CURRENTNODE=new Node("127.0.0.1",2121);
        try {
            ZKService.listenLeaderDataNode();
            int y=0;
        } catch (Exception e) {
            e.printStackTrace();
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
