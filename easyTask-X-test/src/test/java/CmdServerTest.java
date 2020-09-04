import com.github.liuche51.easyTaskX.socket.CmdServer;
import org.junit.Test;

import java.io.IOException;

public class CmdServerTest {
    @Test
    public void test() {
        try {
            CmdServer.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
