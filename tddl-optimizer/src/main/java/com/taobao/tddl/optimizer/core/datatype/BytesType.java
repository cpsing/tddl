package com.taobao.tddl.optimizer.core.datatype;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.primitives.Bytes;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.BaseRowSet;

/**
 * {@link Bytes} 类型
 * 
 * @author mengshi.sunmengshi 2014年1月21日 下午5:16:00
 * @since 5.0.0
 */
public class BytesType extends AbstractDataType<byte[]> {

    @Override
    public Class getDataClass() {
        return byte[].class;
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
            return DataEncoder.calculateEncodedLength(convertFrom(value));
        }
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        try {
            byte[][] data = new byte[0][];
            int length = DataDecoder.decode(bytes, offset, data);
            return new DecodeResult(data[0], length);
        } catch (CorruptEncodingException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public byte[] incr(Object value) {
        throw new NotSupportException("bytes类型不支持incr操作");
    }

    @Override
    public byte[] decr(Object value) {
        throw new NotSupportException("bytes类型不支持decr操作");
    }

    @Override
    public byte[] getMaxValue() {
        return new byte[] { Byte.MAX_VALUE };
    }

    @Override
    public byte[] getMinValue() {
        return new byte[] { Byte.MIN_VALUE };
    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("bytes类型不支持计算操作");
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getBytes(index);
            }

            @Override
            public Object get(BaseRowSet rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
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

        byte[] value = convertFrom(o1);
        byte[] targetValue = convertFrom(o2);
        int notZeroOffset = 0;
        int targetNotZeroOffset = 0;
        for (notZeroOffset = 0; notZeroOffset < value.length; notZeroOffset++) {
            if (value[notZeroOffset] != 0) {
                break;
            }
        }

        for (targetNotZeroOffset = 0; targetNotZeroOffset < targetValue.length; targetNotZeroOffset++) {
            if (targetValue[targetNotZeroOffset] != 0) {
                break;
            }
        }

        int actualLength = value.length - notZeroOffset;
        int actualTargetLength = targetValue.length - targetNotZeroOffset;

        if (actualLength > actualTargetLength) {
            return 1;
        } else if (actualLength < actualTargetLength) {
            return -1;
        } else {
            int index = notZeroOffset;
            int targetIndex = targetNotZeroOffset;
            while (true) {
                if (index >= value.length || targetIndex >= targetValue.length) {
                    break;
                }
                short shortValue = (short) (value[index] & 0xff);
                short shortTargetValue = (short) (targetValue[targetIndex] & 0xff);
                boolean re = (shortValue == shortTargetValue);
                if (re) {
                    index++;
                    targetIndex++;
                    continue;
                } else {
                    return shortValue > shortTargetValue ? 1 : -1;
                }

            }
            return 0;
        }
    }

}
