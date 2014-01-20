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

    public static interface ResultGetter {

        Object get(ResultSet rs, int index) throws SQLException;

        Object get(BaseRowSet rs, int index);
    }

    ResultGetter getResultGetter();

    Object convertFromObject(Object value);

    Object convertToType(Object value, DataType toType);

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

    Object incr(Object value);

    Object decr(Object value);

    Object getMax();

    Object getMin();

}
