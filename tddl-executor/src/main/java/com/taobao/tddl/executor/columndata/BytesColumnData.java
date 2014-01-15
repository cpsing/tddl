package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:01:29
 * @since 5.1.0
 */
public class BytesColumnData extends ColumnData {

    private final byte[] value;

    public BytesColumnData(byte[] value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData cv) {
        if (cv.isNull()) {
            return 1;
        }
        int notZeroOffset = 0;
        int targetNotZeroOffset = 0;
        for (notZeroOffset = 0; notZeroOffset < value.length; notZeroOffset++) {
            if (value[notZeroOffset] != 0) break;
        }

        byte[] targetValue = (byte[]) cv.getValue();

        for (targetNotZeroOffset = 0; targetNotZeroOffset < targetValue.length; targetNotZeroOffset++) {
            if (targetValue[targetNotZeroOffset] != 0) break;
        }

        int actualLength = value.length - notZeroOffset;
        int actualTargetLength = targetValue.length - targetNotZeroOffset;

        if (actualLength > actualTargetLength) {
            return 1;
        } else if (actualLength < actualTargetLength) {
            return -1;
        } else {
            int index = notZeroOffset;
            int targetIndex = targetNotZeroOffset;
            while (true) {

                if (index >= value.length || targetIndex >= targetValue.length) break;
                short shortValue = (short) (this.value[index] & 0xff);
                short shortTargetValue = (short) (targetValue[targetIndex] & 0xff);
                boolean re = (shortValue == shortTargetValue);
                if (re) {
                    index++;
                    targetIndex++;
                    continue;
                } else {
                    return shortValue > shortTargetValue ? 1 : -1;
                }

            }
            return 0;
        }
    }

    @Override
    public ColumnData add(ColumnData val) {
        throw new UnsupportedOperationException("BytesColumnData can not support 'add(ColumnData<Date> val)'");
    }

    @Override
    public byte[] getValue() {
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

        byte[] targetValue = (byte[]) cv.getValue();
        if (this.value.length != targetValue.length) {
            return false;
        }

        for (int i = 0; i < targetValue.length; i++) {
            boolean equal = (this.value[i] == targetValue[i]);
            if (equal) {
                continue;
            } else {
                return false;
            }
        }

        return true;
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
