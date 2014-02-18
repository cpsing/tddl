package com.taobao.tddl.optimizer.core.datatype;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorException;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * @since 5.0.0
 */
public abstract class AbstractDataType<DATA> implements DataType<DATA> {

    protected Class dataClass = null;

    @Override
    public Class getDataClass() {
        if (dataClass == null) {
            Type type = this.getClass().getGenericSuperclass();
            while (type != null && !(type instanceof ParameterizedType)) {
                type = ((Class) type).getGenericSuperclass();
            }

            if (type == null) {
                throw new TddlRuntimeException("you should specify DataType<DATA> for DATA type, ie: DataType<String>");
            }
            dataClass = (Class) getGenericClass((ParameterizedType) type, 0);
        }

        return dataClass;
    }

    @Override
    public DATA convertFrom(Object value) {
        if (value == null) {
            return null;
        } else {
            Convertor convertor = getConvertor(value.getClass());
            if (convertor != null) {
                // 没有convertor代表类型和目标相同，不需要做转化
                return (DATA) convertor.convert(value, getDataClass());
            } else {
                return (DATA) value;
            }
        }
    }

    /**
     * 获取convertor接口
     */
    protected Convertor getConvertor(Class clazz) {
        if (clazz.equals(getDataClass())) {
            return null;
        }

        Convertor convertor = ConvertorHelper.getInstance().getConvertor(clazz, getDataClass());
        if (convertor == null) {
            throw new ConvertorException("Unsupported convert: [" + clazz.getName() + "," + getDataClass().getName()
                                         + "]");
        } else {
            return convertor;
        }
    }

    /**
     * 取得范性信息
     * 
     * @param cls
     * @param i
     * @return
     */
    private Class<?> getGenericClass(ParameterizedType parameterizedType, int i) {
        Object genericClass = parameterizedType.getActualTypeArguments()[i];
        if (genericClass instanceof ParameterizedType) { // 处理多级泛型
            return (Class<?>) ((ParameterizedType) genericClass).getRawType();
        } else if (genericClass instanceof GenericArrayType) { // 处理数组泛型
            return (Class<?>) ((GenericArrayType) genericClass).getGenericComponentType();
        } else {
            return (Class<?>) genericClass;
        }
    }
}
