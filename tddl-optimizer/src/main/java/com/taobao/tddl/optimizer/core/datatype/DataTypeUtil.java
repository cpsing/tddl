package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class DataTypeUtil {

    public static DataType getTypeOfObject(Object v) {

        if (v == null) return DataType.LongType;

        if (v instanceof Integer) return DataType.IntType;
        if (v instanceof Long) return DataType.LongType;
        if (v instanceof Short) return DataType.ShortType;
        if (v instanceof Float) return DataType.FloatType;
        if (v instanceof Double) return DataType.DoubleType;
        if (v instanceof BigDecimal) return DataType.BigDecimalType;

        if (v instanceof String) return DataType.StringType;

        if (v instanceof Date) return DataType.DateType;
        if (v instanceof Time) return DataType.TimeType;
        if (v instanceof Timestamp) return DataType.TimestampType;

        if (v instanceof Byte) return DataType.ByteType;
        if (v instanceof Byte[]) return DataType.BytesType;
        if (v instanceof Boolean) return DataType.BooleanType;

        if (v instanceof Blob) return DataType.BlobType;
        if (v instanceof Clob) return DataType.ClobType;

        throw new TddlRuntimeException("type: " + v.getClass().getSimpleName() + " is not supported");

    }
}
