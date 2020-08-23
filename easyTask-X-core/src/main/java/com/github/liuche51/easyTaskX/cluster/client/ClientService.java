package com.github.liuche51.easyTaskX.cluster.client;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.cluster.Node;
import com.github.liuche51.easyTaskX.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * 客户端服务
 */
public class ClientService {
    private static final Logger log = LoggerFactory.getLogger(ClientService.class);
    /**
     * 更新Broker位置信息
     *
     * @param broker
     * @return
     */
    public static boolean updateBrokerPosition(String broker) {
        try {
            if (StringUtils.isNullOrEmpty(broker)) return false;
            String[] temp = broker.split(":");
            if (temp.length != 2) return false;
            Map<String, Node> clients = ClusterService.CURRENTNODE.getClients();
            Node newclient=new Node(temp[0], Integer.valueOf(temp[1]).intValue());
            clients.put(broker, newclient);
            return true;
        } catch (Exception e) {
            log.error("updateBrokerPosition", e);
            return false;
        }
    }
}
