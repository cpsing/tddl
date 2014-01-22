package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class FloatType extends CommonType<Float> {

    private final Calculator calculator = new Calculator() {

                                            @Override
                                            public Object add(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object sub(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object multiply(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object divide(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object mod(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object and(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object or(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object not(Object v1) {
                                                Float i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object bitAnd(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1.longValue() & i2.longValue();
                                            }

                                            @Override
                                            public Object bitOr(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return i1.longValue() | i2.longValue();
                                            }

                                            @Override
                                            public Object bitNot(Object v1) {
                                                Float i1 = convertFrom(v1);
                                                return ~i1.longValue();
                                            }

                                            @Override
                                            public Object xor(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object bitXor(Object v1, Object v2) {
                                                Float i1 = convertFrom(v1);
                                                Float i2 = convertFrom(v2);
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
        return calculator;
    }

}
