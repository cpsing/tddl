package com.taobao.tddl.optimizer.core.datatype;

public abstract class AbstractDataType implements DataType {

    protected abstract Object convertFromLong(Long value);

    protected abstract Object convertFromShort(Short value);

    protected abstract Object convertFromInteger(Integer value);

    protected abstract Object converFromString(String value);

}
