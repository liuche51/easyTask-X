

import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import org.junit.Test;
import org.sqlite.SQLiteException;

import java.sql.SQLException;
import java.util.*;

public class UnitTest {
    public static List<Thread> threadList = new LinkedList<>();

    @Test
    public void test() throws Exception {
        EasyTaskConfig config=new EasyTaskConfig();
        config.setTaskStorePath("C:/easyTaskX/node1");
        NodeService.setConfig(config);
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

}

