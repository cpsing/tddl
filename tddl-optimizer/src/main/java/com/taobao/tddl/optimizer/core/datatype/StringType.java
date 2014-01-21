package com.taobao.tddl.optimizer.core.datatype;

public class StringType extends AbstractDataType {

    @Override
    public int compare(Object o1, Object o2) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ResultGetter getResultGetter() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String convertFromLong(Long value) {
        return String.valueOf(value);
    }

    @Override
    public String convertFromShort(Short value) {
        return String.valueOf(value);
    }

    @Override
    public String convertFromInteger(Integer value) {
        return String.valueOf(value);
    }

    @Override
    public Object convertToType(Object value, DataType toType) {
        return ((AbstractDataType) toType).convertFromString((String) value);
    }

    @Override
    public String convertFromString(String value) {
        return value;
    }

    @Override
    public String convertFromObject(Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int encodeToBytes(Object value, byte[] dst, int offset) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getLength(Object value) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public DecodeResult decodeFromBytes(byte[] bytes, int offset) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String incr(Object value) {
        String c = convertFromObject(value);
        StringBuilder newStr = new StringBuilder();
        newStr.append(c);
        newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) + 1));
        c = newStr.toString();

        return c;
    }

    @Override
    public Object decr(Object value) {
        String c = convertFromObject(value);
        StringBuilder newStr = new StringBuilder();
        newStr.append(c);
        newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) - 1));
        c = newStr.toString();

        return c;
    }

    @Override
    public Object getMax() {
        return Character.MAX_VALUE + "";
    }

    @Override
    public Object getMin() {
        // TODO Auto-generated method stub
        return null;
    }

}
