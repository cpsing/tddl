package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Date;

import com.taobao.tddl.common.model.BaseRowSet;

public interface DataType<DATA> extends Comparator<Object> {

    public static final DataType<Integer>    IntType        = new IntegerType();
    public static final DataType<Long>       LongType       = null;
    public static final DataType<Short>      ShortType      = null;
    public static final DataType<String>     StringType     = new StringType();
    public static final DataType<Double>     DoubleType     = null;
    public static final DataType<Float>      FloatType      = null;
    public static final DataType<Date>       DateType       = null;
    public static final DataType<Timestamp>  TimestampType  = null;
    public static final DataType<Boolean>    BooleanType    = null;
    public static final DataType<BigDecimal> BigDecimalType = null;
    public static final DataType             DatetimeType   = null;
    public static final DataType             TimeType       = null;

    public static final DataType<Blob>       BlobType       = null;
    public static final DataType<Clob>       ClobType       = null;

    public static final DataType             BitType        = null;
    public static final DataType<Byte[]>     BytesType      = null;
    public static final DataType<Byte>       ByteType       = null;
    public static final DataType             NullType       = null;

    public static interface ResultGetter {

        Object get(ResultSet rs, int index) throws SQLException;

        Object get(BaseRowSet rs, int index);
    }

    ResultGetter getResultGetter();

    /**
     * @param value
     * @param dst
     * @param offset
     * @return encode之后的byte[]的length
     */
    int encodeToBytes(Object value, byte[] dst, int offset);

    /**
     * encode之后的byte[]的length
     * 
     * @param value
     * @return
     */
    int getLength(Object value);

    public class DecodeResult {

        public Object value;
        public int    length;

        public DecodeResult(Object value, int length){
            super();
            this.value = value;
            this.length = length;
        }

    }

    DecodeResult decodeFromBytes(byte[] bytes, int offset);

    /**
     * 针对数据类型获取开区间的下一条
     */
    DATA incr(Object value);

    /**
     * 针对数据类型获取开区间的下一条
     */
    DATA decr(Object value);

    /**
     * 对应数据类型的最大值
     */
    DATA getMaxValue();

    /**
     * 对应数据类型的最小值
     */
    DATA getMinValue();

    /**
     * 将数据转化为当前DataType类型
     */
    DATA convertFrom(Object value);

    /**
     * 数据类型对应的class
     */
    Class getDataClass();

    /**
     * 数据计算器
     */
    Calculator getCalculator();
}
