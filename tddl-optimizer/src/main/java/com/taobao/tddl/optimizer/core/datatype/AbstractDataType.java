package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorException;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * @since 5.1.0
 */
public abstract class AbstractDataType implements DataType {

    @Override
    public Object convertFrom(Object value) {
        if (value == null) {
            return null;
        } else {
            Convertor convertor = getConvertor(value.getClass());
            return convertor.convert(value, getDataClass());
        }
    }

    /**
     * 获取convertor接口
     */
    protected Convertor getConvertor(Class clazz) {
        Convertor convertor = ConvertorHelper.getInstance().getConvertor(clazz, getDataClass());
        if (convertor == null) {
            throw new ConvertorException("not found convertor for : " + clazz.getName() + " to "
                                         + getDataClass().getName());
        }
        return convertor;
    }
}
