package com.taobao.tddl.optimizer.core.datatype;

public class StringType extends AbstractDataType {

    @Override
    public int compare(Object o1, Object o2) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Object add(Object o1, Object o2) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultGetter getResultGetter() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object convertFromLong(Long value) {
        return String.valueOf(value);
    }

    @Override
    public Object convertFromShort(Short value) {
        return String.valueOf(value);
    }

    @Override
    public Object convertFromInteger(Integer value) {
        return String.valueOf(value);
    }

    @Override
    public Object convertToType(Object value, DataType toType) {
        return ((AbstractDataType) toType).converFromString((String) value);
    }

    @Override
    public Object converFromString(String value) {
        return value;
    }

}
