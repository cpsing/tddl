package com.taobao.tddl.executor.columndata;

import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:01:34
 * @since 5.1.0
 */
public abstract class ColumnData implements Comparable<ColumnData> {

    public abstract ColumnData add(ColumnData cv);

    public abstract boolean isNull();

    public abstract Object getValue();

    public abstract boolean equals(ColumnData cv);

    public static ColumnData getColumnData(Object value, DATA_TYPE type) {
        switch (type) {
            case BYTES_VAL:
                return new BytesColumnData((byte[]) value);
            case INT_VAL:
                return new IntColumnData((Integer) value);
        }
    }
}
