package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;

public class DateType extends AbstractDataType<java.util.Date> {

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getDate(index);
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
        Date date = this.convertFrom(value);
        String v = (String) DateType.StringType.convertFrom(value);
        return DataEncoder.encode(v, dst, offset);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 1;
        } else {
            return KeyEncoder.calculateEncodedStringLength((String) value);
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            String[] vs = new String[1];
            int lenght = DataDecoder.decodeString(bytes, offset, vs);
            return new DecodeResult(vs[0], lenght);
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Date incr(Object value) {
        throw new NotSupportException("string类型不支持incr操作");
    }

    @Override
    public Date decr(Object value) {
        throw new NotSupportException("string类型不支持decr操作");
    }

    @Override
    public Date getMaxValue() {
        return null;
    }

    @Override
    public Date getMinValue() {
        return null; // 返回null值
    }

    @Override
    public Class getDataClass() {
        return String.class;
    }

    @Override
    public Date convertFrom(Object value) {
        return (Date) super.convertFrom(value);
    }

    @Override
    public int compare(Object o1, Object o2) {
        return 0;
    }

    @Override
    public Calculator getCalculator() {
        return null;
    }

}
