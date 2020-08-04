package com.github.liuche51.easyTaskX.util;



import com.github.liuche51.easyTaskX.cluster.ClusterService;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {
    public static String getCurrentDateTime(){
      return   ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    public static ZonedDateTime parse(String dateTimeStr){
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
       return ZonedDateTime.parse(dateTimeStr, df.withZone(ZoneId.systemDefault()));
    }
    public static long getTimeStamp(ZonedDateTime dateTime){
        return ZonedDateTime.now().toInstant().toEpochMilli();
    }

    /**
     * zk心跳时间是否超时了死亡时间阈值
     * @param dateTime
     * @param differSecond 时钟差值
     * @return
     */
    public static boolean isGreaterThanDeadTime(String dateTime,long differSecond){
        if(ZonedDateTime.now().minusSeconds(differSecond).minusSeconds(ClusterService.getConfig().getDeadTimeOut())
                .compareTo(DateUtils.parse(dateTime)) > 0)
            return true;
        else return false;
    }

    /**
     * zk心跳时间是否超时了失效时间阈值
     * @param dateTime
     * @param differSecond 时钟差值
     * @return
     */
    public static boolean isGreaterThanLoseTime(String dateTime,long differSecond){
        if(ZonedDateTime.now().minusSeconds(differSecond).minusSeconds(ClusterService.getConfig().getLoseTimeOut())
                .compareTo(DateUtils.parse(dateTime)) > 0)
            return true;
        else return false;
    }
}
