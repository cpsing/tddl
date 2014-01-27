package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;

public class StringType extends AbstractDataType<String> {

    private final Calculator calculator = new Calculator() {

                                            public BigDecimal convertToBigDecimal(Object v) {
                                                return DataType.BigDecimalType.convertFrom(v);
                                            }

                                            @Override
                                            public Object xor(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().xor(d1, d2);
                                            }

                                            @Override
                                            public Object sub(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().sub(d1, d2);
                                            }

                                            @Override
                                            public Object or(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().or(d1, d2);
                                            }

                                            @Override
                                            public Object not(Object v1) {
                                                BigDecimal d1 = convertToBigDecimal(v1);

                                                return DataType.BigDecimalType.getCalculator().not(d1);
                                            }

                                            @Override
                                            public Object multiply(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().multiply(d1, d2);
                                            }

                                            @Override
                                            public Object mod(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().mod(d1, d2);
                                            }

                                            @Override
                                            public Object divide(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().divide(d1, d2);
                                            }

                                            @Override
                                            public Object bitXor(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().bitXor(d1, d2);
                                            }

                                            @Override
                                            public Object bitOr(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().bitOr(d1, d2);
                                            }

                                            @Override
                                            public Object bitNot(Object v1) {
                                                BigDecimal d1 = convertToBigDecimal(v1);

                                                return DataType.BigDecimalType.getCalculator().bitNot(d1);
                                            }

                                            @Override
                                            public Object bitAnd(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().bitAnd(d1, d2);
                                            }

                                            @Override
                                            public Object and(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().and(d1, d2);
                                            }

                                            @Override
                                            public Object add(Object v1, Object v2) {
                                                BigDecimal d1 = convertToBigDecimal(v1);
                                                BigDecimal d2 = convertToBigDecimal(v2);

                                                return DataType.BigDecimalType.getCalculator().add(d1, d2);
                                            }
                                        };

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getString(index);
            }

            @Override
            public Object get(BaseRowSet rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
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
            if (!(value instanceof String)) {
                System.out.println("ok");
            }
            return KeyEncoder.calculateEncodedStringLength((String) value);
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            String[] vs = new String[1];
            int lenght = DataDecoder.decodeString(bytes, offset, vs);
            return new DecodeResult(vs[0], lenght);
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public String incr(Object value) {
        String c = convertFrom(value);
        StringBuilder newStr = new StringBuilder(c);
        newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) + 1));
        return newStr.toString();
    }

    @Override
    public String decr(Object value) {
        String c = convertFrom(value);
        StringBuilder newStr = new StringBuilder(c);
        newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) - 1));
        return newStr.toString();
    }

    @Override
    public String getMaxValue() {
        return new String(new char[] { Character.MAX_VALUE });
    }

    @Override
    public String getMinValue() {
        return null; // 返回null值
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }

        if (o2 == null) {
            return 1;
        }

        String no1 = convertFrom(o1);
        String no2 = convertFrom(o2);

        /**
         * mysql 默认不区分大小写，这里可能是个坑
         */
        return no1.compareToIgnoreCase(no2);
    }

    @Override
    public Calculator getCalculator() {
        return calculator;
    }

}
