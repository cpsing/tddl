package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;

public class StringType extends AbstractDataType<String> {

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getString(index);
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
        return DataEncoder.encode(this.convertFrom(value), dst, offset);
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
    public String incr(Object value) {
        String c = convertFrom(value);
        StringBuilder newStr = new StringBuilder(c);
        newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) + 1));
        return newStr.toString();
    }

    @Override
    public String decr(Object value) {
        String c = convertFrom(value);
        StringBuilder newStr = new StringBuilder(c);
        newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) - 1));
        return newStr.toString();
    }

    @Override
    public String getMaxValue() {
        return new String(new char[] { Character.MAX_VALUE });
    }

    @Override
    public String getMinValue() {
        return null; // 返回null值
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
