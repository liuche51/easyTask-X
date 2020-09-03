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
}
