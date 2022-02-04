import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.util.Util;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class LeaderServiceTest {
    @Test
    public void initSelectFollows() {
        try {
            BrokerService.getConfig().setBackupCount(2);
            BrokerService.CURRENT_NODE=new BaseNode("127.0.0.1",2020);
            //SliceLeaderService.initSelectFollows();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @Test
    public void notifyFollowsLeaderPosition(){
        try {
        List<BaseNode> list=new LinkedList<>();
            BaseNode node1=new BaseNode(Util.getLocalIP(),2021);
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
