package com.taobao.tddl.optimizer.core.datatype;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * @since 5.1.0
 */
public abstract class AbstractDataType<DATA> implements DataType<DATA> {

    @Override
    public Class getDataClass() {
        Type type = this.getClass().getGenericInterfaces()[0];
        if (!(type instanceof ParameterizedType)) {
            // 用户不指定AsyncLoadCallBack的泛型信息
            throw new TddlRuntimeException("you should specify DataType<DATA> for DATA type, ie: DataType<String>");
        }

        return (Class) getGenericClass((ParameterizedType) type, 0);
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
        return ConvertorHelper.getInstance().getConvertor(clazz, getDataClass());
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
