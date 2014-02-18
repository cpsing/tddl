package com.taobao.tddl.common.utils.convertor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * object <-> String 之间的转化器，目前只实现object -> String的转化
 * 
 * @author jianghang 2011-5-25 下午10:26:30
 */
public class StringAndObjectConvertor {

    /**
     * object -> string 转化
     */
    public static class ObjectToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(Clob.class) && destClass.equals(String.class)) {
                if (src == null) {
                    return null;
                } else {
                    Clob clob = (Clob) src;
                    try {
                        InputStream input = clob.getAsciiStream();
                        byte[] bb = new byte[(int) clob.length()];
                        input.read(bb);
                        return bb;
                    } catch (SQLException e) {
                        throw new ConvertorException(e);
                    } catch (IOException e) {
                        throw new ConvertorException(e);
                    }
                }
            } else {
                return src != null ? src.toString() : null;
            }
        }
    }

    /**
     * string -> bytes 转化
     */
    public static class StringToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src) && destClass.equals(byte[].class)) {
                return src != null ? ((String) src).getBytes() : null;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    /**
     * bytes -> String 转化
     */
    public static class BytesToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (byte[].class.isInstance(src) && destClass.equals(String.class)) {
                try {
                    return new String((byte[]) src, "ISO-8859-1");
                } catch (UnsupportedEncodingException e) {
                    throw new ConvertorException(e);
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

}
