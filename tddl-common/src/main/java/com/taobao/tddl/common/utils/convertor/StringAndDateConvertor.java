package com.taobao.tddl.common.utils.convertor;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.taobao.tddl.common.utils.DateUtils;

/**
 * string <-> Date/Calendar 之间的转化
 * 
 * @author jianghang 2011-5-26 上午09:50:31
 */
public class StringAndDateConvertor {

    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT      = "yyyy-MM-dd";
    public static final String TIME_FORMAT      = "HH:mm:ss";

    /**
     * string -> Date
     */
    public static class StringToDate extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) { // 必须是字符串
                return DateUtils.str_to_time((String) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    /**
     * string -> sql Date
     */
    public static class StringToSqlDate extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) { // 必须是字符串
                return ConvertorHelper.dateToSql.convert(DateUtils.str_to_time((String) src), destClass);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    /**
     * string-> Calendar
     */
    public static class StringToCalendar extends StringToDate {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) { // 必须是字符串
                Date dest = (Date) super.convert(src, Date.class);
                Calendar result = new GregorianCalendar();
                result.setTime(dest);
                return result;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    /**
     * Date -> string(格式为："2010-10-01")
     */
    public static class SqlDateToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Date.class.isInstance(src)) { // 必须是Date对象
                return new SimpleDateFormat(DATE_FORMAT).format((Date) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

    }

    /**
     * Date -> string(格式为："00:00:00")
     */
    public static class SqlTimeToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Date.class.isInstance(src)) { // 必须是Date对象
                return new SimpleDateFormat(TIME_FORMAT).format((Date) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

    }

    /**
     * Date -> string(格式为："2010-10-01 00:00:00")
     */
    public static class SqlTimestampToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Date.class.isInstance(src)) { // 必须是Date对象
                return new SimpleDateFormat(TIMESTAMP_FORMAT).format((Date) src);
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

    }

    /**
     * Calendar -> string(格式为："2010-10-01")
     */
    public static class CalendarToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Calendar.class.isInstance(src)) { // 必须是Date对象
                return new SimpleDateFormat(TIMESTAMP_FORMAT).format(((Calendar) src).getTime());
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

    }

}
