package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:02:05
 * @since 5.1.0
 */
public class DoubleColumnData implements ColumnData {

    private final Double value;

    public DoubleColumnData(Double value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData cv) {
        if (cv.isNull()) {
            return 1;
        }

        return this.value.compareTo((Double) cv.getValue());
    }

    @Override
    public ColumnData add(ColumnData val) {
        if (val.isNull()) {
            return this;
        }

        DoubleColumnData da = new DoubleColumnData(this.value + (Double) val.getValue());
        return da;
    }

    @Override
    public Double getValue() {
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
