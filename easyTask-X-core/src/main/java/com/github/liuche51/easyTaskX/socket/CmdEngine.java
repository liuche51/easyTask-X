package com.github.liuche51.easyTaskX.socket;

import com.github.liuche51.easyTaskX.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class CmdEngine {
    public static Map<String,Object> CMD;
    private static Map<String,Object> getcmd;
    static {
        getcmd=new HashMap<String,Object>(){
            {
                put("brokerRegisterInfo","");
                put("clinetRegisterInfo","");

            }
        };
        CMD=new HashMap<String,Object>(){
            {
                put("get",getcmd);

            }
        };
    }
    public static Command parse(String cmd){
        if(StringUtils.isNullOrEmpty(cmd))
            return null;
        String[] params=cmd.split(" ");
        if(params.length<2)
            return null;
        Command cm=new Command();
        Object next=null;
        String[] pa=new String[params.length-2];
        for(int i=0;i<params.length;i++){
            if(i==0){
                next=CMD.get(params[0]);
                if(next!=null){
                    cm.setType(params[0]);
                }
            }else if(i==1){
                Map<String,Object> map=( Map<String,Object>)next;
                next=map.get(params[1]);
                if(next!=null){
                    cm.setMethod(params[1]);
                }
            }else if(i>=2){
                pa[i-2]=params[i];
            }
        }
        cm.setParams(pa);
        return cm;
    }
}
