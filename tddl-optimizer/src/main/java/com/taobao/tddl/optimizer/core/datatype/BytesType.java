package com.taobao.tddl.optimizer.core.datatype;

import com.google.common.primitives.Bytes;
import com.taobao.tddl.common.exception.TddlRuntimeException;

/**
 * {@link Bytes} 类型
 * 
 * @author mengshi.sunmengshi 2014年1月21日 下午5:16:00
 * @since 5.1.0
 */
public class BytesType extends CommonType<Byte> {

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
        return null;
    }

}
