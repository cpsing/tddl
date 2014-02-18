package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Comparator;

import com.taobao.tddl.common.model.BaseRowSet;

/**
 * http://dev.mysql.com/doc/refman/5.6/en/type-conversion.html
 * 
 * <pre>
 * 类型信息主要针对两类
 * a. 字段  【数据库结构里可以获取】
 * b. 函数
 *    1. Dummy函数 (下推) 【返回null，由应用基于返回值调用{@linkplain DataTypeUtil}进行动态获取】
 *    2. tddl自实现 【以函数的第一个字段的类型为准，嵌套函数时递归获取第一个字段】
 *    
 * 针对特殊的函数，比如select 1+1, 没有字段信息，默认以LongType处理，
 * ps. mysql中string的加法也是按照数字来处理, 比如 select "a" + "b" = 2 ,  select "1" + "a" = 1
 * </pre>
 */
public interface DataType<DATA> extends Comparator<Object> {

    public static final DataType<Integer>            IntegerType    = new IntegerType();
    public static final DataType<Long>               LongType       = new LongType();
    public static final DataType<Short>              ShortType      = new ShortType();
    public static final DataType<String>             StringType     = new StringType();
    public static final DataType<Double>             DoubleType     = new DoubleType();
    public static final DataType<Float>              FloatType      = new FloatType();
    public static final DataType<Date>               DateType       = new DateType();
    public static final DataType<Timestamp>          TimestampType  = new TimestampType();
    public static final DataType<Boolean>            BooleanType    = new BooleanType();
    public static final DataType<BigInteger>         BigIntegerType = new BigIntegerType();
    public static final DataType<BigDecimal>         BigDecimalType = new BigDecimalType();
    public static final DataType<java.sql.Timestamp> DatetimeType   = new TimestampType();
    public static final DataType<java.sql.Time>      TimeType       = new TimeType();

    public static final DataType<byte[]>             BlobType       = new BlobType();
    public static final DataType<String>             ClobType       = new ClobType();

    public static final DataType<Integer>            BitType        = new BitType();
    public static final DataType<byte[]>             BytesType      = new BytesType();
    public static final DataType<Byte>               ByteType       = new ByteType();
    public static final DataType                     NullType       = LongType;

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
