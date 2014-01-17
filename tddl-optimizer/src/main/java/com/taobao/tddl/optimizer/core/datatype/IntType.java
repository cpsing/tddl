package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.model.BaseRowSet;

public class IntType extends NumberType {

    @Override
    public int compare(Object o1, Object o2) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Object add(Object o1, Object o2) {
        // TODO Auto-generated method stub
        return null;
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
                if (val == null) return 0;

                if (val instanceof Integer) return val;

                if (val instanceof Number) return ((Number) val).intValue();

                if (val instanceof BigDecimal) return ((BigDecimal) val).intValue();

                String strVal = rs.getString(index);

                return Integer.valueOf(strVal);
            }

        };
    }

    @Override
    public Object convertFromLong(Long value) {
        return value.intValue();
    }

    @Override
    public Object convertFromShort(Short value) {
        return value.intValue();
    }

    @Override
    public Object convertFromInteger(Integer value) {
        return value;
    }

    @Override
    public Object convertToType(Object value, DataType toType) {
        return ((AbstractDataType) toType).convertFromInteger((Integer) value);
    }

    @Override
    public Object converFromString(String value) {
        if (value == null) {
            return 0;
        }
        return Integer.valueOf(value);
    }

    @Override
    public byte[] encodeToBytes(Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object decodeFromBytes(byte[] bytes) {
        // TODO Auto-generated method stub
        return null;
    }

}
