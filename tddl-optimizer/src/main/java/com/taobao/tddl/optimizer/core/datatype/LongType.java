package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class LongType extends CommonType<Long> {

    private final Calculator calculator = new Calculator() {

                                            @Override
                                            public Object add(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object sub(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object multiply(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object divide(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object mod(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object and(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object or(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object not(Object v1) {
                                                Long i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object bitAnd(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object bitOr(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object bitNot(Object v1) {
                                                Long i1 = convertFrom(v1);
                                                return ~i1;
                                            }

                                            @Override
                                            public Object xor(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object bitXor(Object v1, Object v2) {
                                                Long i1 = convertFrom(v1);
                                                Long i2 = convertFrom(v2);
                                                return i1 ^ i2;
                                            }
                                        };

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
        return calculator;
    }

}
