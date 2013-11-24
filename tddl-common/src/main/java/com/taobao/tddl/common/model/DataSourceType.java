package com.taobao.tddl.common.model;

public enum DataSourceType {
    DruidDataSource(1);

    private int i;

    private DataSourceType(int i){
        this.i = i;
    }

    public int value() {
        return this.i;
    }

    public static DataSourceType valueOf(int i) {
        for (DataSourceType t : values()) {
            if (t.value() == i) {
                return t;
            }
        }
        throw new IllegalArgumentException("Invalid DataSouceType:" + i);
    }
}
