import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dao.BinlogScheduleDao;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dao.SqliteHelper;
import com.github.liuche51.easyTaskX.dto.db.BinlogSchedule;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import com.github.liuche51.easyTaskX.util.DbTableName;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class UnitTest {
    public static List<Thread> threadList = new LinkedList<>();

    @Test
    public void test() throws Exception {
        EasyTaskConfig config=new EasyTaskConfig();
        config.setTaskStorePath("C:/easyTaskX/node1");
        BrokerService.setConfig(config);
        ScheduleBak bak=new ScheduleBak();
        bak.setId("12345678");
        bak.setTransactionId("12345678");
        try {
            ScheduleBakDao.save(bak);
            ScheduleBakDao.save(bak);
        }catch (SQLException e){
            String message = e.getMessage();
            if(message!=null&&message.contains("SQLITE_CONSTRAINT_PRIMARYKEY")){
                e.printStackTrace();
            }
        }

    }
    @Test
    public void test2() throws Exception {
        EasyTaskConfig config=new EasyTaskConfig();
        config.setTaskStorePath("C:/easyTaskX/node1");
        BrokerService.setConfig(config);
        SqliteHelper helper=new SqliteHelper(DbTableName.SCHEDULE);
        List<BinlogSchedule> schedules=new ArrayList<>();
        BinlogSchedule schedul=new BinlogSchedule();
        schedul.setStatus(1);
        schedul.setScheduleId("111");
        schedul.setSql("");
        schedules.add(schedul);
        Long aLong = BinlogScheduleDao.saveBatch(schedules, helper);
        //BinlogScheduleDao.save("","111",1,helper);
    }
}

