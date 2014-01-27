package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@link Double}类型
 * 
 * @author jianghang 2014-1-21 下午3:28:33
 * @since 5.0.0
 */
public class DoubleType extends CommonType<Double> {

    private final Calculator calculator = new Calculator() {

                                            @Override
                                            public Object add(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object sub(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object multiply(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object divide(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object mod(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object and(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object or(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object not(Object v1) {
                                                Double i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object bitAnd(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1.longValue() & i2.longValue();
                                            }

                                            @Override
                                            public Object bitOr(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1.longValue() | i2.longValue();
                                            }

                                            @Override
                                            public Object bitNot(Object v1) {
                                                Double i1 = convertFrom(v1);
                                                return ~i1.longValue();
                                            }

                                            @Override
                                            public Object xor(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object bitXor(Object v1, Object v2) {
                                                Double i1 = convertFrom(v1);
                                                Double i2 = convertFrom(v2);
                                                return i1.longValue() ^ i2.longValue();
                                            }
                                        };

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
    public Calculator getCalculator() {
        return calculator;
    }

}
