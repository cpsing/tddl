package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class ShortType extends CommonType {

    @Override
    public Class getDataClass() {
        return Short.class;
    }

    @Override
    public Short convertFrom(Object value) {
        return (Short) super.convertFrom(value);
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
            return 3;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Short v = DataDecoder.decodeShortObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Short incr(Object value) {
        return (short) (convertFrom(value) + 1);
    }

    @Override
    public Short decr(Object value) {
        return (short) (convertFrom(value) - 1);
    }

    @Override
    public Short getMaxValue() {
        return Short.MAX_VALUE;
    }

    @Override
    public Short getMinValue() {
        return Short.MIN_VALUE;
    }

}
