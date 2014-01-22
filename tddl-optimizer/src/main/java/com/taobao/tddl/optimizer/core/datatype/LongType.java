package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@linkplain Long}类型
 * 
 * @author jianghang 2014-1-22 上午10:41:28
 * @since 5.1.0
 */
public class LongType extends CommonType<Long> {

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        return DataEncoder.encode(this.convertFrom(value), dst, offset);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 1;
        } else {
            return 9;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Long v = DataDecoder.decodeLongObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Long incr(Object value) {
        return convertFrom(value) + 1;
    }

    @Override
    public Long decr(Object value) {
        return convertFrom(value) - 1;
    }

    @Override
    public Long getMaxValue() {
        return Long.MAX_VALUE;
    }

    @Override
    public Long getMinValue() {
        return Long.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return null;
    }

}
