package com.github.liuche51.easyTaskX.socket;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.monitor.ClusterMonitor;

import java.util.HashMap;
import java.util.Map;

public class CmdService {
    public static Map<String,String> SERVICE;
    static {
        SERVICE=new HashMap<String,String>(){
            {
                put("allregister","getBrokerRegisterInfo");
                put("allregister","getClinetRegisterInfo");
            }
        };
    }
    public static String excuteCmd(String cmd){
        String interfaces=SERVICE.get(cmd);
        switch (interfaces){
            case "getBrokerRegisterInfo":
                return JSONObject.toJSONString(ClusterMonitor.getBrokerRegisterInfo());
            case "getClinetRegisterInfo":
                return JSONObject.toJSONString(ClusterMonitor.getClinetRegisterInfo());
            default:return null;
        }
    }
}
