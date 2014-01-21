package com.taobao.tddl.optimizer.core.datatype;

import java.sql.Clob;

public class ClobType extends AbstractDataType<Clob> {

    @Override
    public com.taobao.tddl.optimizer.core.datatype.DataType.ResultGetter getResultGetter() {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob decr(Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob getMaxValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Clob getMinValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Calculator getCalculator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int compare(Object o1, Object o2) {
        // TODO Auto-generated method stub
        return 0;
    }

}
