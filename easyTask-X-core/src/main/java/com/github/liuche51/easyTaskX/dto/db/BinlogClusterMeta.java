package com.github.liuche51.easyTaskX.dto.db;

/**
 * 集群元数据Binlog日志
 */
public class BinlogClusterMeta {
    private Long id;
    private String optype;
    private String regnode;
    private String key;
    private String value;
    private String createTime;

    public BinlogClusterMeta() {
    }

    public BinlogClusterMeta(String optype, String regnode, String key, String value) {
        this.optype = optype;
        this.regnode = regnode;
        this.key = key;
        this.value = value;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOptype() {
        return optype;
    }

    public void setOptype(String optype) {
        this.optype = optype;
    }

    public String getRegnode() {
        return regnode;
    }

    public void setRegnode(String regnode) {
        this.regnode = regnode;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
