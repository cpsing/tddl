package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@link BigDecimal}类型
 * 
 * @author jianghang 2014-1-21 下午1:54:11
 * @since 5.1.0
 */
public class BigDecimalType extends CommonType {

    private static final BigDecimal maxValue = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal minValue = BigDecimal.valueOf(Long.MIN_VALUE);

    @Override
    public BigDecimal convertFrom(Object value) {
        return (BigDecimal) super.convertFrom(value);
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
            return KeyEncoder.calculateEncodedLength((BigDecimal) value);
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            BigDecimal[] vs = new BigDecimal[1];
            int lenght = DataDecoder.decode(bytes, offset, vs);
            return new DecodeResult(vs[0], lenght);
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public BigDecimal incr(Object value) {
        return convertFrom(value).add(BigDecimal.ONE);
    }

    @Override
    public BigDecimal decr(Object value) {
        return convertFrom(value).subtract(BigDecimal.ONE);
    }

    @Override
    public BigDecimal getMaxValue() {
        return maxValue;
    }

    @Override
    public BigDecimal getMinValue() {
        return minValue;
    }

    @Override
    public Class getDataClass() {
        return BigDecimal.class;
    }

}
