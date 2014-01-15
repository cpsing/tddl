package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:02:28
 * @since 5.1.0
 */
public class NullColumnData implements ColumnData {

    @Override
    public int compareTo(ColumnData cv) {
        if (cv.isNull()) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public ColumnData add(ColumnData val) {
        return val;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public boolean equals(ColumnData cv) {
        if (cv.getValue() == null) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public String toString() {
        return "null";
    }
}
