import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.dto.db.Schedule;
import com.github.liuche51.easyTaskX.util.Util;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 集群测试。模拟三个节点的伪集群
 */
public class ClusterTest {
    private static Logger log = LoggerFactory.getLogger(ClusterTest.class);

    @Test
    public void startNode1() {
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/easyTaskX/node1");
            config.setServerPort(2021);
            config.setCmdPort(3031);
            initData(config,"Node1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode2() {
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/easyTaskX/node2");
            config.setServerPort(2022);
            config.setCmdPort(3032);
            initData(config,"Node2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode3() {
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/easyTaskX/node3");
            config.setServerPort(2023);
            config.setCmdPort(3033);
            initData(config,"Node3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode4() {
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/easyTaskX/node4");
            config.setServerPort(2024);
            config.setCmdPort(3034);
            initData(config,"Node4");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void initData(EasyTaskConfig config,String name) throws Exception {
        config.getAdvanceConfig().setSQLlitePoolSize(5);
        config.setZkAddress("127.0.0.1:2181");
        NodeService.start(config);
        Schedule schedule=new Schedule();
        schedule.setId(Util.generateUniqueId());
        schedule.setClassPath("XXXXX");
        schedule.setExecuteTime(152000000000l);
        Map<String, String> param = new HashMap<String, String>() {
            {
                put("name", name);
                put("birthday", "1988-1-1");
                put("age", "25");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };
        schedule.setParam(JSONObject.toJSONString(param));
        schedule.setPeriod(10);
        schedule.setSource("127.0.0.1:"+ NodeService.getConfig().getServerPort());
        schedule.setTaskType("PERIOD");
        schedule.setUnit("SECONDS");
        //ClusterService.submitTask(schedule);
        //JUnit默认是非守护线程启动和Main方法不同。这里防止当前主线程退出导致子线程也退出了
        while (true) {
            Thread.sleep(2000);
            try {
            } catch (Exception e) {
                e.printStackTrace();
            }
             printinfo();
        }
    }
    private void printinfo() {
        //log.info("集群节点信息：" + ClusterMonitor.getCURRENT_NODEInfo());
        //log.info("数据库连接池信息：" + ClusterMonitor.getSqlitePoolInfo());
        //log.info("Broker注册表:"+JSONObject.toJSONString(ClusterMonitor.getBrokerRegisterInfo()));
    }
}
