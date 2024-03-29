import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.monitor.ClusterMonitor;
import com.github.liuche51.easyTaskX.util.LogUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群测试。模拟三个节点的伪集群
 */
public class ClusterConcurrentTest {

    @Test
    public void startNode1() {

    }

    @Test
    public void startNode2() {

    }

    @Test
    public void startNode3() {

    }

    @Test
    public void startNode4() {

    }

    private void printinfo() {
        //LogUtil.info("集群节点信息：" + ClusterMonitor.getCURRENT_NODEInfo());
        LogUtil.info("Netty客户端连接池信息："+JSONObject.toJSONString(ClusterMonitor.getNettyClientPoolInfo()));
    }
}
