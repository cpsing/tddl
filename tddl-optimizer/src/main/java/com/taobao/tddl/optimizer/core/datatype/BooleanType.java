package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;

public class BooleanType extends CommonType<Boolean> {

    private final Calculator calculator = new Calculator() {

                                            @Override
                                            public Object add(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) + (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object sub(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) - (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object multiply(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) * (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object divide(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);

                                                if (!i2) {
                                                    throw new TddlRuntimeException("false不能作为除数");
                                                }
                                                return (i1 ? 1 : 0) / (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object mod(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1 ? 1 : 0) % (i2 ? 1 : 0);
                                            }

                                            @Override
                                            public Object and(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1) && (i2);
                                            }

                                            @Override
                                            public Object or(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return (i1) || (i2);
                                            }

                                            @Override
                                            public Object not(Object v1) {
                                                Boolean i1 = convertFrom(v1);

                                                return !i1;
                                            }

                                            @Override
                                            public Object bitAnd(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object bitOr(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object bitNot(Object v1) {
                                                Boolean i1 = convertFrom(v1);
                                                return !i1;
                                            }

                                            @Override
                                            public Object xor(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 ^ i2;
                                            }

                                            @Override
                                            public Object bitXor(Object v1, Object v2) {
                                                Boolean i1 = convertFrom(v1);
                                                Boolean i2 = convertFrom(v2);
                                                return i1 ^ i2;
                                            }
                                        };

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
        return calculator;
    }

}
