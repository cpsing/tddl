package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;

public class BooleanType extends CommonType<Boolean> {

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        DataEncoder.encode(this.convertFrom(value), dst, offset);
        return getLength(null);
    }

    @Override
    public int getLength(Object value) {
        return 1;
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Boolean v = DataDecoder.decodeBooleanObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Boolean incr(Object value) {
        throw new NotSupportException("boolean类型不支持incr操作");
    }

    @Override
    public Boolean decr(Object value) {
        throw new NotSupportException("boolean类型不支持decr操作");
    }

    @Override
    public Boolean getMaxValue() {
        return Boolean.TRUE; // 1代表true
    }

    @Override
    public Boolean getMinValue() {
        return Boolean.FALSE; // 0代表false
    }

    @Override
    public Calculator getCalculator() {
        return null;
    }

}
