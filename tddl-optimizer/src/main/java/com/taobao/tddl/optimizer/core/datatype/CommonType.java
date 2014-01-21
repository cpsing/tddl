package com.taobao.tddl.optimizer.core.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.model.BaseRowSet;
import com.taobao.tddl.common.utils.convertor.Convertor;

/**
 * 常见类型的转化处理
 * 
 * @author jianghang 2014-1-21 上午12:39:45
 * @since 5.1.0
 */
public abstract class CommonType extends AbstractDataType {

    private Convertor convertor = null;

    public CommonType(){
        convertor = getConvertor(getDataClass()); // number类型之间的互相转换
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

        Comparable no1 = convertFrom(o1);
        Comparable no2 = convertFrom(o2);

        return ((Comparable) no1).compareTo(no2);
    }

    @Override
    public Comparable convertFrom(Object value) {
        if (value == null) {
            return null;
        } else {
            return (Comparable) convertor.convert(value, getDataClass());
        }
    }

}
