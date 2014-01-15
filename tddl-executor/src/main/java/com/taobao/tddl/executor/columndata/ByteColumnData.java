package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:01:22
 * @since 5.1.0
 */
public class ByteColumnData implements ColumnData {

    private final Byte value;

    public ByteColumnData(Byte value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData cv) {
        if (cv.isNull()) {
            return 1;
        }
        short shortValue = (short) (this.value & 0xff);
        short shortTargetValue = (short) (((Byte) cv.getValue()) & 0xff);

        if (shortValue == shortTargetValue) return 0;
        return shortValue > shortTargetValue ? 1 : -1;
    }

    @Override
    public ColumnData add(ColumnData val) {
        if (val.isNull()) {
            return this;
        }

        byte targetValue = (Byte) val.getValue();
        ByteColumnData da = new ByteColumnData((byte) (this.value + targetValue));
        return da;
    }

    @Override
    public Byte getValue() {
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
