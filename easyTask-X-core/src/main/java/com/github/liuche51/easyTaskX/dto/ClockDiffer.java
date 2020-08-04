package com.github.liuche51.easyTaskX.dto;

import com.alibaba.fastjson.annotation.JSONField;

import java.time.ZonedDateTime;

/**
 * 与目标主机的时钟差距
 */
public class ClockDiffer {
    /**
     * 当前节点与目标节点时间差值。单位秒
     * 正数代表目标节点时钟比当前节点慢
     */
    private long differSecond=0;
    /**
     * 是否已经同步过
     */
    private boolean hasSync=false;
    /**
     * 最近一次同步时间
     */
    @JSONField(format="yyyy-MM-dd HH:mm:ss")
    private ZonedDateTime lastSyncDate=null;

    public long getDifferSecond() {
        return differSecond;
    }

    public void setDifferSecond(long differSecond) {
        this.differSecond = differSecond;
    }

    public boolean isHasSync() {
        return hasSync;
    }

    public void setHasSync(boolean hasSync) {
        this.hasSync = hasSync;
    }

    public ZonedDateTime getLastSyncDate() {
        return lastSyncDate;
    }

    public void setLastSyncDate(ZonedDateTime lastSyncDate) {
        this.lastSyncDate = lastSyncDate;
    }
}
