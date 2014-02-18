package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;
import com.taobao.tddl.common.utils.convertor.Convertor;

/**
 * {@link Timestamp}类型
 * 
 * @author jianghang 2014-1-21 下午5:36:26
 * @since 5.0.0
 */
public class TimestampType extends AbstractDataType<java.sql.Timestamp> {

    private static final Timestamp maxTimestamp = Timestamp.valueOf("9999-12-31 23:59:59");
    private static final Timestamp minTimestamp = Timestamp.valueOf("1900-01-01 00:00:00");
    private Convertor              longToDate   = null;

    public TimestampType(){
        longToDate = this.getConvertor(Long.class);
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getTimestamp(index);
            }

            @Override
            public Object get(BaseRowSet rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        return DataEncoder.encode(DataType.LongType.convertFrom(value), dst, offset);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 1;
        } else {
            return 9;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            String[] vs = new String[1];
            int lenght = DataDecoder.decodeString(bytes, offset, vs);
            Timestamp date = (Timestamp) longToDate.convert(vs[0], getDataClass());
            return new DecodeResult(date, lenght);
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Timestamp incr(Object value) {
        return new Timestamp(((Timestamp) value).getTime() + 1l);
    }

    @Override
    public Timestamp decr(Object value) {
        return new Timestamp(((Timestamp) value).getTime() - 1l);
    }

    @Override
    public Timestamp getMaxValue() {
        return maxTimestamp;
    }

    @Override
    public Timestamp getMinValue() {
        return minTimestamp;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }

        if (o2 == null) {
            return 1;
        }

        Timestamp d1 = convertFrom(o1);
        Timestamp d2 = convertFrom(o2);
        return d1.compareTo(d2);
    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("时间类型不支持算术符操作");
    }

}
