package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class FloatType extends CommonType<Float> {

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        DataEncoder.encode(this.convertFrom(value), dst, offset);
        return getLength(null);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 4;
        } else {
            return 4;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Float v = DataDecoder.decodeFloatObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Float incr(Object value) {
        return this.convertFrom(value) + 0.000001f;
    }

    @Override
    public Float decr(Object value) {
        return this.convertFrom(value) - 0.000001f;
    }

    @Override
    public Float getMaxValue() {
        return Float.MAX_VALUE;
    }

    @Override
    public Float getMinValue() {
        return Float.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        // TODO Auto-generated method stub
        return null;
    }

}
