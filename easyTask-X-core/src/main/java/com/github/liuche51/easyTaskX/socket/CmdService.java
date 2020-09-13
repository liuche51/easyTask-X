package com.github.liuche51.easyTaskX.socket;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.monitor.ClusterMonitor;

import java.util.HashMap;
import java.util.Map;

public class CmdService {
    public static String excuteCmd(Command cmd){
        switch (cmd.getType()){
            case "get":
                switch (cmd.getMethod()){
                    case "brokerRegisterInfo":
                        return JSONObject.toJSONString(ClusterMonitor.getBrokerRegisterInfo());
                    case "clinetRegisterInfo":
                        return JSONObject.toJSONString(ClusterMonitor.getClinetRegisterInfo());
                }
        }
        return "unknown command!";
    }
}
