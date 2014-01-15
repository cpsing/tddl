package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:02:17
 * @since 5.1.0
 */
public class IntColumnData implements ColumnData {

    protected final Integer value;

    public IntColumnData(Integer value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData o) {
        if (o.isNull()) {
            return 1;
        }

        return this.value.compareTo((Integer) o.getValue());
    }

    @Override
    public ColumnData add(ColumnData cv) {
        if (cv.isNull()) {
            return this;
        }

        IntColumnData ic = new IntColumnData(this.value + (Integer) cv.getValue());
        return ic;
    }

    @Override
    public Integer getValue() {
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
