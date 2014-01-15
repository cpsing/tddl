package com.taobao.tddl.executor.columndata;

import java.math.BigInteger;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:01:14
 * @since 5.1.0
 */
public class BigIntColumnData implements ColumnData {

    protected final BigInteger value;

    public BigIntColumnData(BigInteger value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData o) {
        if (o.isNull()) {
            return 1;
        }

        return this.value.compareTo((BigInteger) o.getValue());
    }

    @Override
    public ColumnData add(ColumnData cv) {
        if (cv.isNull()) {
            return this;
        }

        BigIntColumnData ic = new BigIntColumnData(this.value.add((BigInteger) cv.getValue()));
        return ic;
    }

    @Override
    public BigInteger getValue() {
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
