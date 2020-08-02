import com.github.liuche51.easyTaskX.monitor.ClusterMonitor;

import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.core.TaskType;
import com.github.liuche51.easyTaskX.core.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 集群测试。模拟三个节点的伪集群
 */
public class ClusterTest {
    private static Logger log = LoggerFactory.getLogger(ClusterTest.class);

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
        //log.info("集群节点信息：" + ClusterMonitor.getCurrentNodeInfo());
        log.info("数据库连接池信息：" + ClusterMonitor.getSqlitePoolInfo());
    }
}
