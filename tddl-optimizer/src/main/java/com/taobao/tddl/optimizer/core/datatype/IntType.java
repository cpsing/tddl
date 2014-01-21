package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;

public class IntType extends NumberType {

    public Class getDataClass() {
        return Integer.class;
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                // TODO Auto-generated method stub
                return rs.getInt(index);
            }

            @Override
            public Object get(BaseRowSet rs, int index) {
                Object val = rs.getObject(index);

                try {
                    val = convertFromObject(val);

                    return val;
                } catch (Exception ex) {

                }
                String strVal = rs.getString(index);

                return Integer.valueOf(strVal);
            }

        };
    }

    @Override
    public Integer convertFromLong(Long value) {
        return value.intValue();
    }

    @Override
    public Integer convertFromShort(Short value) {
        return value.intValue();
    }

    @Override
    public Integer convertFromInteger(Integer value) {
        return value;
    }

    @Override
    public Integer convertFromString(String value) {
        if (value == null) {
            return 0;
        }
        return Integer.valueOf(value);
    }

    @Override
    public Integer convertFromObject(Object value) {
        if (value == null || value instanceof NullValue) return null;

        if (value instanceof Integer) return (Integer) value;

        if (value instanceof Number) return ((Number) value).intValue();

        if (value instanceof String) return Integer.valueOf((String) value);

        if (value instanceof BigDecimal) return ((BigDecimal) value).intValue();

        throw new RuntimeException("unsupported type: " + value.getClass().getSimpleName() + " converted to integer");
    }

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        Integer v = this.convertFromObject(value);
        return DataEncoder.encode(v, dst, offset);

    }

    @Override
    public int getLength(Object value) {

        if (value == null) return 1;

        return 5;

    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Integer v = DataDecoder.decodeIntegerObj(bytes, offset);

            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Integer incr(Object value) {
        return convertFromObject(value) + 1;
    }

    @Override
    public Integer decr(Object value) {
        return convertFromObject(value) - 1;
    }

    @Override
    public Object getMax() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Object getMin() {
        // TODO Auto-generated method stub
        return null;
    }

}
