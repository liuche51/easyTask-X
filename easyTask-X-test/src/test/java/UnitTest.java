

import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.NodeService;
import com.github.liuche51.easyTaskX.dao.ScheduleBakDao;
import com.github.liuche51.easyTaskX.dto.db.ScheduleBak;
import org.junit.Test;
import org.sqlite.SQLiteException;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
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
    // 文件随机位置写入  耗时：1000ms
    public static void randomWrite1(String path,String content) throws Exception {
        RandomAccessFile raf=new RandomAccessFile(path,"rw");
        Random random=new Random();
        for(int i=0;i<100000;i++){
            raf.seek(random.nextInt((int)raf.length())); // 在文件随机位置写入覆盖
            raf.write((i+content+System.lineSeparator()).getBytes());
        }
        raf.close();
    }
    // 文件尾部位置写入  耗时：800ms
    public static void randomWrite2(String path,String content) throws Exception {
        RandomAccessFile raf=new RandomAccessFile(path,"rw");
        for(int i=0;i<100000;i++){
            raf.seek(raf.length()); // 总是在文件尾部追加
            raf.write((i+content+System.lineSeparator()).getBytes());
        }
        raf.close();
    }
}

