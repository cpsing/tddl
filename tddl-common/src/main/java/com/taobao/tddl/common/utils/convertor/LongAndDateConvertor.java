package com.taobao.tddl.common.utils.convertor;

import java.util.Date;

/**
 * Long <-> Date对象之间的转换
 * 
 * @author jianghang 2014-2-18 下午1:59:25
 * @since 5.0.0
 */
public class LongAndDateConvertor {

    public static class LongToDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Long.class.isInstance(src)) { // 必须是Date类型
                Long time = (Long) src;
                // java.util.Date
                if (destClass.equals(java.util.Date.class)) {
                    return new java.util.Date(time);
                }

                // java.sql.Date
                if (destClass.equals(java.sql.Date.class)) {
                    return new java.sql.Date(time);
                }

                // java.sql.Time
                if (destClass.equals(java.sql.Time.class)) {
                    return new java.sql.Time(time);
                }

                // java.sql.Timestamp
                if (destClass.equals(java.sql.Timestamp.class)) {
                    return new java.sql.Timestamp(time);
                }

            }
            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class DateToLongConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Date.class.isInstance(src)) { // 必须是Date类型
                Date date = (Date) src;
                return date.getTime();
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

}
