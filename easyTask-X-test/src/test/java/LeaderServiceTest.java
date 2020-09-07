import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.cluster.leader.SliceLeaderService;

import com.github.liuche51.easyTaskX.util.Util;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class LeaderServiceTest {
    @Test
    public void initSelectFollows() {
        try {
            ClusterService.getConfig().setBackupCount(2);
            ClusterService.CURRENTNODE=new Node("127.0.0.1",2020);
            //SliceLeaderService.initSelectFollows();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @Test
    public void notifyFollowsLeaderPosition(){
        try {
        List<Node> list=new LinkedList<>();
        Node node1=new Node(Util.getLocalIP(),2021);
        list.add(node1);
       // LeaderUtil.notifyFollowsLeaderPosition(list,3);
            while (true){
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
