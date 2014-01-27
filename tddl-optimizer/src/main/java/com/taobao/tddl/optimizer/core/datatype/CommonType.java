package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.model.BaseRowSet;
import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorException;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * 常见类型的转化处理
 * 
 * @author jianghang 2014-1-21 上午12:39:45
 * @since 5.0.0
 */
public abstract class CommonType<DATA> extends AbstractDataType<DATA> {

    private Convertor convertor       = null;
    private Convertor stringConvertor = null;

    public CommonType(){
        convertor = ConvertorHelper.commonToCommon;
        stringConvertor = ConvertorHelper.stringToCommon;
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                Class clazz = getDataClass();
                if (clazz.equals(Integer.class)) {
                    return rs.getInt(index);
                } else if (clazz.equals(Short.class)) {
                    return rs.getShort(index);
                } else if (clazz.equals(Long.class)) {
                    return rs.getLong(index);
                } else if (clazz.equals(Boolean.class)) {
                    return rs.getBoolean(index);
                } else if (clazz.equals(Byte.class)) {
                    return rs.getByte(index);
                } else if (clazz.equals(Float.class)) {
                    return rs.getFloat(index);
                } else if (clazz.equals(Double.class)) {
                    return rs.getDouble(index);
                } else if (clazz.equals(BigInteger.class)) {
                    return rs.getBigDecimal(index).toBigInteger();
                } else if (clazz.equals(BigDecimal.class)) {
                    return rs.getBigDecimal(index);
                } else {
                    throw new NotSupportException("不支持的类型");
                }
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

        DATA no1 = convertFrom(o1);
        DATA no2 = convertFrom(o2);
        return ((Comparable) no1).compareTo(no2);
    }

    @Override
    public DATA convertFrom(Object value) {
        if (value == null) {
            return null;
        } else {
            try {
                try {
                    return (DATA) convertor.convert(value, getDataClass());
                } catch (ConvertorException e) {
                    Convertor cv = getConvertor(value.getClass());
                    if (cv != null) {
                        return (DATA) cv.convert(value, getDataClass());
                    } else {
                        return (DATA) value;
                    }
                }
            } catch (NumberFormatException e) {
                // mysql中针对不可识别的字符，返回数字0
                return (DATA) stringConvertor.convert("0", getDataClass());
            }
        }
    }

}
