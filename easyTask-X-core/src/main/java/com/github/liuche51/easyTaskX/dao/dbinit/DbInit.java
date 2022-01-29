package com.github.liuche51.easyTaskX.dao.dbinit;

import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.*;
import com.github.liuche51.easyTaskX.util.DbTableName;
import com.github.liuche51.easyTaskX.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DbInit {
    public static boolean hasInit = false;//数据库是否已经初始化

    /**
     * 数据库初始化。需要避免多线程
     *
     * @return
     */
    public static synchronized boolean init() {
        if (hasInit)
            return true;
        try {
            //创建db存储文件夹
            File file = new File(NodeService.getConfig().getTaskStorePath());
            if (!file.exists()) {
                file.mkdirs();
            }
            ScheduleInit.initSchedule();
            ScheduleInit.initBinlog();
            ScheduleInit.initHistory();
            ScheduleBakInit.initSchedule();
            LogInit.initLogError();
            ClusterMetadataBinlogInit.initClusterMetadataBinlog();
            hasInit = true;
            LogUtil.info("Sqlite DB 初始化完成");
            return true;
        } catch (Exception e) {
            LogUtil.error("easyTask db init fail.", e);
            return false;
        }
    }
}
