package com.taobao.tddl.common.utils.convertor;

import java.sql.Blob;
import java.sql.SQLException;

/**
 * Blob <-> bytes类型之间的转化
 * 
 * @author jianghang 2014-1-21 下午6:15:01
 * @since 5.0.0
 */
public class BlobAndBytesConvertor {

    /**
     * Blob -> bytes 转化
     */
    public static class BlobToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Blob.class.isInstance(src) && destClass.equals(byte[].class)) {
                if (src == null) {
                    return null;
                } else {
                    try {
                        Blob blob = (Blob) src;
                        return blob.getBytes(0, (int) blob.length());
                    } catch (SQLException e) {
                        throw new ConvertorException(e);
                    }
                }
            }

            return src != null ? src.toString() : null;
        }
    }

}
