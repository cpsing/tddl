package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * int/Integer类型
 * 
 * @since 5.1.0
 */
public class IntegerType extends CommonType {

    @Override
    public Class getDataClass() {
        return Integer.class;
    }

    @Override
    public Integer convertFrom(Object value) {
        return (Integer) super.convertFrom(value);
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
            return 5;
        }
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
        return convertFrom(value) + 1;
    }

    @Override
    public Integer decr(Object value) {
        return convertFrom(value) - 1;
    }

    @Override
    public Integer getMaxValue() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Integer getMinValue() {
        return Integer.MIN_VALUE;
    }

}
