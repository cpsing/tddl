package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import com.taobao.tddl.common.model.BaseRowSet;

public interface DataType extends Comparator<Object> {

    public final static DataType IntType        = new IntType();
    public static final DataType LongType       = null;
    public static final DataType ShortType      = null;

    public final static DataType StringType     = new StringType();

    public static final DataType DoubleType     = null;
    public static final DataType FloatType      = null;

    public static final DataType DateType       = null;
    public static final DataType TimestampType  = null;

    public static final DataType BooleanType    = null;
    public static final DataType BigDecimalType = null;
    public static final DataType DatetimeType   = null;
    public static final DataType TimeType       = null;
    public static final DataType BlobType       = null;
    public static final DataType BitType        = null;
    public static final DataType BytesType      = null;

    Object add(Object o1, Object o2);

    public static interface ResultGetter {

        Object get(ResultSet rs, int index) throws SQLException;

        Object get(BaseRowSet rs, int index);
    }

    ResultGetter getResultGetter();

    Object converFromObject(Object value);

    Object convertToType(Object value, DataType toType);

    byte[] encodeToBytes(Object value);

    Object decodeFromBytes(byte[] bytes);

    Object incr(Object value);

    Object decr(Object value);

    Object getMax();

    Object getMin();

}
