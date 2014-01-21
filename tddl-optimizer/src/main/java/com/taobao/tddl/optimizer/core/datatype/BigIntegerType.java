package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigInteger;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@link BigInteger}类型
 * 
 * @author jianghang 2014-1-21 下午1:49:09
 * @since 5.1.0
 */
public class BigIntegerType extends CommonType {

    private static final BigInteger maxValue = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger minValue = BigInteger.valueOf(Long.MIN_VALUE);

    @Override
    public BigInteger convertFrom(Object value) {
        return (BigInteger) super.convertFrom(value);
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
            return KeyEncoder.calculateEncodedLength((BigInteger) value);
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            BigInteger[] vs = new BigInteger[1];
            int lenght = DataDecoder.decode(bytes, offset, vs);
            return new DecodeResult(vs[0], lenght);
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public BigInteger incr(Object value) {
        return convertFrom(value).add(BigInteger.ONE);
    }

    @Override
    public BigInteger decr(Object value) {
        return convertFrom(value).subtract(BigInteger.ONE);
    }

    @Override
    public BigInteger getMaxValue() {
        return maxValue;
    }

    @Override
    public BigInteger getMinValue() {
        return minValue;
    }

    @Override
    public Class getDataClass() {
        return BigInteger.class;
    }

}
