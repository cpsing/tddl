package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:02:34
 * @since 5.1.0
 */
public class ShortColumnData implements ColumnData {

    private final Short value;

    public ShortColumnData(Short value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData cv) {
        // TODO:目标是null
        if (cv.isNull()) {
            return 1;
        }

        return this.value.compareTo((Short) cv.getValue());
    }

    @Override
    public ColumnData add(ColumnData cv) {
        if (cv.isNull()) {
            return this;
        }

        ShortColumnData ic = new ShortColumnData((short) (this.value + (Short) cv.getValue()));
        return ic;
    }

    @Override
    public Short getValue() {
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
