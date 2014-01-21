package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@link Double}类型
 * 
 * @author jianghang 2014-1-21 下午3:28:33
 * @since 5.1.0
 */
public class DoubleType extends CommonType {

    @Override
    public Double convertFrom(Object value) {
        return (Double) super.convertFrom(value);
    }

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        DataEncoder.encode(this.convertFrom(value), dst, offset);
        return getLength(null);
    }

    @Override
    public int getLength(Object value) {
        if (value == null) {
            return 8;
        } else {
            return 8;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Double v = DataDecoder.decodeDoubleObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Double incr(Object value) {
        return this.convertFrom(value) + 0.000001d;
    }

    @Override
    public Double decr(Object value) {
        return this.convertFrom(value) - 0.000001d;
    }

    @Override
    public Double getMaxValue() {
        return Double.MAX_VALUE;
    }

    @Override
    public Double getMinValue() {
        return Double.MIN_VALUE;
    }

    @Override
    public Class getDataClass() {
        return Double.class;
    }

}
