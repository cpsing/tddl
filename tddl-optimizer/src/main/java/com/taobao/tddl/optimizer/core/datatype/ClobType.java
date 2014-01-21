package com.taobao.tddl.optimizer.core.datatype;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;

public class ClobType extends AbstractDataType<Clob> {

    @Override
    public com.taobao.tddl.optimizer.core.datatype.DataType.ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(BaseRowSet rs, int index) {
                return rs.getObject(index);
            }

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getClob(index);
            }
        };
    }

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getLength(Object value) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public com.taobao.tddl.optimizer.core.datatype.DataType.DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob incr(Object value) {
        throw new TddlRuntimeException("不支持clob类型的比较");
    }

    @Override
    public Clob decr(Object value) {
        throw new TddlRuntimeException("不支持clob类型的比较");

    }

    @Override
    public Clob getMaxValue() {
        throw new TddlRuntimeException("不支持clob类型的比较");

    }

    @Override
    public Clob getMinValue() {
        throw new TddlRuntimeException("不支持clob类型的比较");

    }

    @Override
    public Calculator getCalculator() {
        throw new TddlRuntimeException("不支持clob类型的计算");

    }

    @Override
    public int compare(Object o1, Object o2) {
        throw new TddlRuntimeException("不支持clob类型的比较");
    }

}
