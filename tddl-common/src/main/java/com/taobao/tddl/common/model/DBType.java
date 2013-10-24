package com.taobao.tddl.common.model;

/**
 * @author linxuan
 */
public enum DBType {

    ORACLE(0), MYSQL(1);

    private int i;

    private DBType(int i){
        this.i = i;
    }

    public int value() {
        return this.i;
    }

    public static DBType valueOf(int i) {
        for (DBType t : values()) {
            if (t.value() == i) {
                return t;
            }
        }
        throw new IllegalArgumentException("Invalid SqlType:" + i);
    }
}
