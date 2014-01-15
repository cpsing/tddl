package com.taobao.tddl.executor.columndata;

import java.math.BigDecimal;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:01:59
 * @since 5.1.0
 */
public class DecimalColumnData implements ColumnData {

    private final BigDecimal value;

    public DecimalColumnData(BigDecimal value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData cv) {
        if (cv.isNull()) {
            return 1;
        }

        return this.value.compareTo((BigDecimal) cv.getValue());
    }

    @Override
    public ColumnData add(ColumnData val) {
        if (val.isNull()) {
            return this;
        }

        DecimalColumnData da = new DecimalColumnData(this.value.add((BigDecimal) val.getValue()));
        return da;
    }

    @Override
    public BigDecimal getValue() {
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
