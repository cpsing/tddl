package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * number类型的转化处理, TODO 可使用属性的方式cache convertor
 * 
 * @author jianghang 2014-1-21 上午12:39:45
 * @since 5.1.0
 */
public abstract class NumberType extends AbstractDataType {

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

        Number no1 = convertFromObject(o1);
        Number no2 = convertFromObject(o2);
        if (no1 instanceof Comparable && no2 instanceof Comparable) {
            return ((Comparable) no1).compareTo(no2);
        } else {
            throw new NotSupportException("不支持的类型：" + no1.getClass() + " compare " + no2.getClass());
        }
    }

    @Override
    public Number convertFromObject(Object value) {
        if (value == null) {
            return null;
        } else {
            Convertor convertor = ConvertorHelper.getInstance().getConvertor(value.getClass(), getDataClass());
            return (Number) convertor.convert(value, getDataClass());
        }
    }

    @Override
    public Object convertToType(Object value, DataType toType) {
        if (value == null) {
            return null;
        } else {
            Convertor convertor = ConvertorHelper.getInstance().getConvertor(value.getClass(), getDataClass());
            return convertor.convert(value, toType.getDataClass());
        }
    }

    @Override
    protected Number convertFromLong(Long value) {
        return convertFromObject(value);
    }

    @Override
    protected Number convertFromShort(Short value) {
        return convertFromObject(value);
    }

    @Override
    protected Number convertFromInteger(Integer value) {
        return convertFromObject(value);
    }

    @Override
    protected Number convertFromString(String value) {
        return convertFromObject(value);
    }

}
