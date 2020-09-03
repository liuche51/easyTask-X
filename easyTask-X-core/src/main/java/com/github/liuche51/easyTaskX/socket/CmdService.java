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
                put("allregister","getAllRegisterInfo");
            }
        };
    }
    public static String excuteCmd(String cmd){
        String interfaces=SERVICE.get(cmd);
        switch (interfaces){
            case "getAllRegisterInfo":
                return JSONObject.toJSONString(ClusterMonitor.getAllNodeRegisterInfo());
            default:return null;
        }
    }
}
