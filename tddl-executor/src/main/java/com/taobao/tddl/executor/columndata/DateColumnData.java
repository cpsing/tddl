package com.taobao.tddl.executor.columndata;

import java.sql.Date;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:01:52
 * @since 5.1.0
 */
public class DateColumnData implements ColumnData {

    private final Date value;

    public DateColumnData(Date value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData cv) {
        if (cv.isNull()) {
            return 1;
        }

        return this.value.compareTo((java.util.Date) cv.getValue());
    }

    @Override
    public ColumnData add(ColumnData val) {
        throw new UnsupportedOperationException("DateColumnData can not support 'add(ColumnData val)'");

    }

    @Override
    public Date getValue() {
        return this.value;
    }

    @Override
    public boolean equals(ColumnData cv) {
        if (cv.isNull() && this.value != null) {
            return false;
        } else if (!cv.isNull() && this.value == null) {
            return false;
        } else if (cv.isNull() && this.value == null) {
            return true;
        }

        if (cv.getValue().equals(this.value)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isNull() {
        return this.value == null;
    }

    @Override
    public String toString() {
        if (this.isNull()) return "null";
        return this.value.toString();
    }
}
