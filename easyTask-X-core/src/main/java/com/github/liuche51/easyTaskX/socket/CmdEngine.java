package com.github.liuche51.easyTaskX.socket;

import com.github.liuche51.easyTaskX.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class CmdEngine {
    public static Map<String,Object> CMD;
    static {
        CMD=new HashMap<String,Object>(){
            {
                put("get","allregister");

            }
        };
    }
    public static String parse(String cmd){
        if(StringUtils.isNullOrEmpty(cmd))
            return null;
        String[] params=cmd.split(" ");
        for(int i=0;i<params.length;i++){
            Object obj=CMD.get(params[i].toLowerCase());
            if(obj instanceof String)
                return obj.toString();
        }
        return null;
    }
}
