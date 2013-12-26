package com.taobao.tddl.qatest.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Comment for DateUtil
 * <p/>
 * Author By: manhong.yqd Created Date: 2010-12-2 ����01:43:03
 */
public class DateUtil {

    // logger
    private static Log                        log                 = LogFactory.getLog(DateUtil.class);

    /** yyyy/MM/dd HH:mm:ss */
    public static final String                DATETIME_FULLSLASH  = "yyyy/MM/dd HH:mm:ss";                                                      // 19
    /** yyyy-MM-dd HH:mm:ss */
    public static final String                DATETIME_FULLHYPHEN = "yyyy-MM-dd HH:mm:ss";
    /** yyyy/MM/dd HH:mm */
    public static final String                DATETIME_HM_SLASH   = "yyyy/MM/dd HH:mm";                                                         // 16
    /** yyyy-MM-dd HH:mm */
    public static final String                DATETIME_HM_HYPHEN  = "yyyy-MM-dd HH:mm";
    /** yyyy/MM/dd */
    public static final String                DATE_FULLSLASH      = "yyyy/MM/dd";                                                               // 10
    /** yyyy-MM-dd */
    public static final String                DATE_FULLHYPHEN     = "yyyy-MM-dd";
    /** yyyy/MM */
    public static final String                DATE_YM_SLASH       = "yyyy/MM";                                                                  // 7
    /** yyyy-MM */
    public static final String                DATE_YM_HYPHEN      = "yyyy-MM";
    /** HH:mm:ss */
    public static final String                TIME_HMS_HYPHEN     = "HH:mm:ss";
    /** ������ʽ:yyyy/MM/dd HH:mm:ss ���� yyyy-MM-dd HH:mm:ss */
    private static final String               V_DATETIME_FULL     = "[0-9]{4}[\\/-]{1}[0-9]{2}[\\/-]{1}[0-9]{2} [0-9]{2}\\:[0-9]{2}\\:[0-9]{2}"; // 19
    /** ������ʽ:yyyy/MM/dd HH:mm ���� yyyy-MM-dd HH:mm */
    private static final String               V_DATETIME_HM_FULL  = "[0-9]{4}[\\/-]{1}[0-9]{2}[\\/-]{1}[0-9]{2} [0-9]{2}\\:[0-9]{2}";           // 13
    /** ������ʽ:yyyy/MM/dd ���� yyyy-MM-dd */
    private static final String               V_DATE_FULL         = "[0-9]{4}[\\/-]{1}[0-9]{2}[\\/-]{1}[0-9]{2}";                               // 10
    /** ������ʽ:yyyy/MM ���� yyyy-MM */
    private static final String               V_DATE_YM           = "[0-9]{4}[\\/-]{1}[0-9]{2}";                                                // 7

    private static final Map<Integer, String> VALIDATE_MAP        = Collections.synchronizedMap(new HashMap<Integer, String>());

    static {
        VALIDATE_MAP.put(DATETIME_FULLHYPHEN.length(), V_DATETIME_FULL);
        VALIDATE_MAP.put(DATETIME_HM_HYPHEN.length(), V_DATETIME_HM_FULL);
        VALIDATE_MAP.put(DATE_FULLHYPHEN.length(), V_DATE_FULL);
        VALIDATE_MAP.put(DATE_YM_HYPHEN.length(), V_DATE_YM);
    }

    /**
     * ������ת����ָ����ʽ���ַ�
     * 
     * @param date
     * @param pattern
     * @return
     */
    public static String formatDate(Date date, String pattern) {
        if (date == null) {
            return null;
        }
        if (StringUtils.isBlank(pattern)) {
            return null;
        }
        SimpleDateFormat fmt = new SimpleDateFormat(pattern);
        String convStr = fmt.format(date);
        return convStr;
    }

    /**
     * ���ض���ʽ���ַ�ת��������
     * 
     * @param inDate
     * @return
     */
    public static Date parseDate(String inDate, String convPattern) {
        if (StringUtils.isBlank(inDate)) {
            return null;
        }
        if (convPattern == null) {
            return null;
        }
        if (!validate(inDate)) {
            return null;
        }
        SimpleDateFormat formatter = new SimpleDateFormat(convPattern);
        formatter.setLenient(false);
        Date date = null;
        try {
            date = formatter.parse(inDate);
        } catch (ParseException e) {
            log.warn("date parse error", e);
        }
        return date;
    }

    /**
     * ��Date������ת����Calendar
     * 
     * @param date
     * @return
     */
    public static Calendar convertCalendar(Date date) {
        Calendar calendar = null;
        if (date != null) {
            calendar = Calendar.getInstance();
            calendar.setTime(date);
        }
        return calendar;
    }

    /**
     * ��Calendar������ת����Date
     * 
     * @param cal
     * @return
     */
    public static Date convertDate(Calendar cal) {
        Date date = null;
        if (cal != null) {
            date = cal.getTime();
        }
        return date;
    }

    /**
     * ���ڼ���
     * 
     * @param inData
     * @return
     */
    private static boolean validate(String inData) {
        if (StringUtils.isEmpty(inData)) {
            return false;
        }
        String checkPattern = VALIDATE_MAP.get(inData.length());
        if (checkPattern == null) {
            return false;
        }
        boolean isValidate = inData.matches(checkPattern);
        return isValidate;
    }

    /**
     * ���ص������һ����ַ�����
     * 
     * @param date
     * @return
     */
    public static String getLastDay(Date date) {
        Calendar cal = convertCalendar(date);
        int value = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, value);
        return formatDate(cal.getTime(), DATE_FULLHYPHEN);
    }

    /**
     * ��time��ճ�00:00:00
     * 
     * @param date
     * @return
     */
    public static String truncDate(Date date) {
        Calendar cal = convertCalendar(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return formatDate(cal.getTime(), DATETIME_FULLHYPHEN);
    }

    /**
     * �ж��Ƿ�Ϊ����
     * 
     * @param year
     * @return
     */
    public static boolean isLeapYear(int year) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, 2, 1);
        calendar.add(Calendar.DATE, -1);
        if (calendar.get(Calendar.DAY_OF_MONTH) == 29) {
            System.out.println(year + " year is a leap year.");
            return true;
        } else {
            System.out.println(year + " year is not a leap year.");
            return false;
        }
    }

    /**
     * �������/���
     * 
     * @param Date date
     * @param Date date1
     * @return ��������������
     */
    public static int getDiffDate(Date date, Date date1) {
        return (int) ((date.getTime() - date1.getTime()) / (24 * 3600 * 1000));
    }

    /**
     * �������/���
     * 
     * @param Calendar date
     * @param Calendar date1
     * @return ��������������
     */
    public static int getDiffDate(Calendar date, Calendar date1) {
        return getDiffDate(date.getTime(), date1.getTime());
    }

    /**
     * �������/���
     * 
     * @param intervals
     * @param format
     * @return
     */
    public static String getDiffDate(int intervals, String format) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, intervals);
        Date intervalDay = cal.getTime();
        return formatDate(intervalDay, format);
    }

    /**
     * �������/���
     * 
     * @param calendar
     * @param offset
     * @return
     */
    public static Calendar getDiffDate(Calendar calendar, int offset) {
        calendar.set(Calendar.DAY_OF_YEAR, (calendar.get(Calendar.DAY_OF_YEAR) + offset));
        return calendar;
    }

    /**
     * �������/���
     * 
     * @param intervals
     * @return
     */
    public static Date getDiffDate(Date date, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_YEAR, (calendar.get(Calendar.DAY_OF_YEAR) + offset));
        return calendar.getTime();
    }

    /**
     * �������/���
     * 
     * @param intervals
     * @return
     */
    public static Date getDiffDate(int intervals) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, intervals);
        return cal.getTime();
    }

    /**
     * �����һ������
     * 
     * @param date
     * @return
     */
    public static Date getMonday(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return c.getTime();
    }

    /**
     * ������������
     * 
     * @param date
     * @return
     */
    public static String getFriday(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY);
        return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
    }

    /**
     * �ж����������Ƿ���ͬһ��
     * 
     * @param date1
     * @param date2
     * @return
     */
    static boolean isSameWeekDates(Date date1, Date date2) {
        long diff = getMonday(date1).getTime() - getMonday(date2).getTime();
        if (Math.abs(diff) < 1000 * 60 * 60 * 24) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * �ж����������Ƿ���ͬһ��
     * 
     * @param date1
     * @param date2
     * @return
     */
    boolean isSameWeekDates2(Date date1, Date date2) {
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();
        cal1.setTime(date1);
        cal2.setTime(date2);
        int subYear = cal1.get(Calendar.YEAR) - cal2.get(Calendar.YEAR);
        if (0 == subYear) {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR)) return true;
        } else if (1 == subYear && 11 == cal2.get(Calendar.MONTH)) {
            // ���12�µ����һ�ܺ�������һ�ܵĻ������һ�ܼ���������ĵ�һ��
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR)) return true;
        } else if (-1 == subYear && 11 == cal1.get(Calendar.MONTH)) {
            if (cal1.get(Calendar.WEEK_OF_YEAR) == cal2.get(Calendar.WEEK_OF_YEAR)) return true;
        }
        return false;
    }

    /**
     * ������
     * 
     * @param d1
     * @param d2
     * @param freeDays
     * @return
     */
    public static int getWorkDay(Date d1, Date d2, int[] freeDays) {
        int dNum = 0;
        dNum = (int) ((d2.getTime() - d1.getTime()) / 1000 / 60 / 60 / 24) + 1;

        return dNum - getFreeDay(d1, dNum, freeDays);
    }

    /**
     * ��Ϣ��
     * 
     * @param date
     * @param dNum
     * @param freeDays
     * @return
     */
    public static int getFreeDay(Date date, int dNum, int[] freeDays) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int start = cal.get(Calendar.DAY_OF_WEEK) - 1;
        int freeNum = 0;
        for (int i = 0; i < dNum; i++) {
            for (int j = 0; j < freeDays.length; j++) {
                if ((start + i) % 7 == freeDays[j]) {
                    freeNum++;
                }
            }
        }
        return freeNum;
    }

    /**
     * ������
     * 
     * @param d1
     * @param d2
     * @return
     */
    public static int getWorkDay(Date d1, Date d2) {
        int[] freeDays = { 0, 6 };//default: Sunday and Saturday are the free days.
        return getWorkDay(d1, d2, freeDays);
    }

    /**
     * ��Ϣ��
     * 
     * @param date
     * @param dNum
     * @return
     */
    public static int getFreeDay(Date date, int dNum) {
        int[] freeDays = { 0, 6 };//default: Sunday and Saturday are the free days.
        return getFreeDay(date, dNum, freeDays);
    }

    /**
     * ����������
     * 
     * @return
     */
    public static String getSeqWeek() {
        Calendar c = Calendar.getInstance(Locale.CHINA);
        String week = Integer.toString(c.get(Calendar.WEEK_OF_YEAR));
        if (week.length() == 1) week = "0" + week;
        String year = Integer.toString(c.get(Calendar.YEAR));
        return year + week;
    }

    /**
     * �������
     * 
     * @param date
     * @return
     */
    public static int getYear(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.get(java.util.Calendar.YEAR);
    }

    /**
     * �����·�
     * 
     * @param date
     * @return
     */
    public static int getMonth(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.get(java.util.Calendar.MONTH) + 1;
    }

    /**
     * �����շ�
     * 
     * @param date
     * @return
     */
    public static int getDay(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.get(java.util.Calendar.DAY_OF_MONTH);
    }

    /**
     * ����Сʱ
     * 
     * @param date
     * @return
     */
    public static int getHour(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.get(java.util.Calendar.HOUR_OF_DAY);
    }

    /**
     * ���ط���
     * 
     * @param date
     * @return
     */
    public static int getMinute(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.get(java.util.Calendar.MINUTE);
    }

    /**
     * ��������
     * 
     * @param date
     * @return
     */
    public static int getSecond(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.get(java.util.Calendar.SECOND);
    }

    /**
     * ���غ���
     * 
     * @param date
     * @return
     */
    public static long getMillis(java.util.Date date) {
        java.util.Calendar c = java.util.Calendar.getInstance();
        c.setTime(date);
        return c.getTimeInMillis();
    }

    /**
     * ����java.sql.Date
     * 
     * @param date
     * @return
     */
    public static java.sql.Date parseSqlDate(Date date) {
        if (date != null) return new java.sql.Date(date.getTime());
        else return null;
    }

    /**
     * ����java.sql.Date
     * 
     * @param dateStr
     * @param format
     * @return
     */
    public static java.sql.Date parseSqlDate(String dateStr, String format) {
        java.util.Date date = parseDate(dateStr, format);
        return parseSqlDate(date);
    }

    /**
     * ����java.sql.Date
     * 
     * @param dateStr
     * @return
     */
    public static java.sql.Date parseSqlDate(String dateStr) {
        return parseSqlDate(dateStr, "yyyy/MM/dd");
    }

}
