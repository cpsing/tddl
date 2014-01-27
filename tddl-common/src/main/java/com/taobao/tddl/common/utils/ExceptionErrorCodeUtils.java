package com.taobao.tddl.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午10:41:51
 * @since 5.0.0
 */
public class ExceptionErrorCodeUtils {

    public static final String separator                               = "-!@#-";
    public static final int    Duplicate_entry                         = 10000;
    public static final int    Null_Pointer_exception                  = 10001;
    public static final int    Wrong_PassWD_Or_UserName                = 10002;
    public static final int    UNKNOWN_EXCEPTION                       = 10003;
    public static final int    WRITE_NOT_ALLOW_EXECUTE_ON_MUTI_SERVERS = 10004;
    /**
     * 2开头的，是需要重试，并且是写库重试的异常
     */
    public static final int    Read_only                               = 20001;
    /**
     * 3开头的，是需要读重试的异常
     */
    public static final int    Communication_link_failure              = 30000;
    public static final int    Connect_timeout                         = 30001;

    public static Integer getErrorCode(String exception) {
        if (exception == null || exception.isEmpty()) {
            return null;
        }
        String[] errcodeWithException = exception.split(separator);
        if (errcodeWithException.length > 2) {
            throw new IllegalArgumentException("can't understand this exception: " + exception);
        }
        if (errcodeWithException.length < 2) {
            return null;
        }
        try {
            return Integer.valueOf(errcodeWithException[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("error exception can't be understand ." + exception, e);
        }
    }

    public static String appendErrorCode(Integer errorCode, String exception) {
        StringBuilder sb = new StringBuilder();
        sb.append(errorCode).append(separator).append(exception);
        return sb.toString();
    }

    public static String exceptionToString(Throwable ex) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        PrintStream ps = new PrintStream(out);
        ex.printStackTrace(ps);
        return new String(out.toByteArray());
    }

    public static String appendErrorCode(Integer errorCode, Throwable ex) {
        return appendErrorCode(errorCode, exceptionToString(ex));
    }

}
