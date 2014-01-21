package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * int/Integer类型
 * 
 * @since 5.1.0
 */
public class IntegerType extends CommonType<Integer> {

    private Calculator calculator = new Calculator() {

                                      @Override
                                      public Object add(Object v1, Object v2) {
                                          Integer i1 = convertFrom(v1);
                                          Integer i2 = convertFrom(v2);
                                          return i1 + i2;
                                      }

                                      @Override
                                      public Object sub(Object v1, Object v2) {
                                          // TODO Auto-generated method stub
                                          return null;
                                      }

                                      @Override
                                      public Object multiply(Object v1, Object v2) {
                                          // TODO Auto-generated method stub
                                          return null;
                                      }

                                      @Override
                                      public Object divide(Object v1, Object v2) {
                                          // TODO Auto-generated method stub
                                          return null;
                                      }

                                      @Override
                                      public Object mod(Object v1, Object v2) {
                                          // TODO Auto-generated method stub
                                          return null;
                                      }

                                      @Override
                                      public Object and(Object v1, Object v2) {
                                          // TODO Auto-generated method stub
                                          return null;
                                      }

                                      @Override
                                      public Object or(Object v1, Object v2) {
                                          // TODO Auto-generated method stub
                                          return null;
                                      }

                                      @Override
                                      public Object not(Object v1) {
                                          // TODO Auto-generated method stub
                                          return null;
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
            return 5;
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            Integer v = DataDecoder.decodeIntegerObj(bytes, offset);
            return new DecodeResult(v, getLength(v));
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public Integer incr(Object value) {
        return convertFrom(value) + 1;
    }

    @Override
    public Integer decr(Object value) {
        return convertFrom(value) - 1;
    }

    @Override
    public Integer getMaxValue() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Integer getMinValue() {
        return Integer.MIN_VALUE;
    }

    @Override
    public Calculator getCalculator() {
        return null;
    }

}
