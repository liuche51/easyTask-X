package com.github.liuche51.easyTaskX.util;


import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtils {
    public static String getCurrentDateTime() {
        return ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public static ZonedDateTime parse(String dateTimeStr) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return ZonedDateTime.parse(dateTimeStr, df.withZone(ZoneId.systemDefault()));
    }

    public static ZonedDateTime parse(long timeMini) {
        Timestamp timestamp = new Timestamp(timeMini);
        return ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());
    }

    public static long getTimeStamp(ZonedDateTime dateTime) {
        return ZonedDateTime.now().toInstant().toEpochMilli();
    }

    /**
     * 集群系欸但心跳时间是否超时了失效时间阈值
     *
     * @param dateTime
     * @return
     */
    public static boolean isGreaterThanLoseTime(ZonedDateTime dateTime) {
        if (ZonedDateTime.now().minusMinutes(BrokerService.getConfig().getAdvanceConfig().getLoseTimeOut())
                .compareTo(dateTime) > 0)
            return true;
        else return false;
    }

    /**
     * 当前时间与目标时间比较，是否超时XX秒了
     *
     * @param compareTime 目标比较时间
     * @param second      超时时间。单位秒
     * @return
     */
    public static boolean isGreaterThanSomeTime(ZonedDateTime compareTime, int second) {
        if (ZonedDateTime.now().minusSeconds(second)
                .compareTo(compareTime) > 0)
            return true;
        else return false;
    }
}
