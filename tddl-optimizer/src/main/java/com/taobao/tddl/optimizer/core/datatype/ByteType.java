package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@link Byte} 类型
 * 
 * @author jianghang 2014-1-21 下午3:28:17
 * @since 5.1.0
 */
public class ByteType extends CommonType<Byte> {

    private final Calculator calculator = new Calculator() {

                                            @Override
                                            public Object add(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 + i2;
                                            }

                                            @Override
                                            public Object sub(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 - i2;
                                            }

                                            @Override
                                            public Object multiply(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 * i2;
                                            }

                                            @Override
                                            public Object divide(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 / i2;
                                            }

                                            @Override
                                            public Object mod(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 % i2;
                                            }

                                            @Override
                                            public Object and(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return (i1 != 0) && (i2 != 0);
                                            }

                                            @Override
                                            public Object or(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return (i1 != 0) || (i2 != 0);
                                            }

                                            @Override
                                            public Object not(Object v1) {
                                                Byte i1 = convertFrom(v1);

                                                return i1 == 0;
                                            }

                                            @Override
                                            public Object bitAnd(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 & i2;
                                            }

                                            @Override
                                            public Object bitOr(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return i1 | i2;
                                            }

                                            @Override
                                            public Object bitNot(Object v1) {
                                                Byte i1 = convertFrom(v1);
                                                return ~i1;
                                            }

                                            @Override
                                            public Object xor(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
                                                return (i1 != 0) ^ (i2 != 0);
                                            }

                                            @Override
                                            public Object bitXor(Object v1, Object v2) {
                                                Byte i1 = convertFrom(v1);
                                                Byte i2 = convertFrom(v2);
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
            return 2;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Byte v = DataDecoder.decodeByteObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Byte incr(Object value) {
        return Byte.valueOf(((Integer) (this.convertFrom(value).intValue() + 1)).byteValue());
    }

    @Override
    public Byte decr(Object value) {
        return Byte.valueOf(((Integer) (this.convertFrom(value).intValue() - 1)).byteValue());
    }

    @Override
    public Byte getMaxValue() {
        return Byte.MAX_VALUE;
    }

    @Override
    public Byte getMinValue() {
        return Byte.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

}
