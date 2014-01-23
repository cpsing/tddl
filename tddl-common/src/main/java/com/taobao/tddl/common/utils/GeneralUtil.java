package com.taobao.tddl.common.utils;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.BooleanUtils;

import com.taobao.tddl.common.exception.TddlException;

/**
 * 公共方便方法
 * 
 * @author whisper
 */
public class GeneralUtil {

    static Pattern pattern = Pattern.compile("\\d+$");

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.size() == 0;
    }

    public static boolean isNotEmpty(Collection collection) {
        return collection != null && collection.size() != 0;
    }

    public static String getTab(int count) {
        StringBuffer tab = new StringBuffer();
        for (int i = 0; i < count; i++)
            tab.append("    ");
        return tab.toString();
    }

    public static String getExtraCmdString(Map<String, Object> extraCmd, String key) {
        if (extraCmd == null) {
            return null;
        }

        if (key == null) {
            return null;
        }
        Object obj = extraCmd.get(key);
        if (obj != null) {
            return obj.toString().trim();
        } else {
            return null;
        }
    }

    public static boolean getExtraCmdBoolean(Map<String, Object> extraCmd, String key, boolean defaultValue) {
        String value = getExtraCmdString(extraCmd, key);
        if (value == null) {
            return defaultValue;
        } else {
            return BooleanUtils.toBoolean(value);
        }
    }

    public static long getExtraCmdLong(Map<String, Object> extraCmd, String key, long defaultValue) {
        String value = getExtraCmdString(extraCmd, key);
        if (value == null) {
            return defaultValue;
        } else {
            return Long.valueOf(value);
        }
    }

    public static void checkInterrupted() throws TddlException {
        if (Thread.interrupted()) {
            throw new TddlException(new InterruptedException());
        }
    }

    public static void printlnToStringBuilder(StringBuilder sb, String v) {
        sb.append(v).append("\n");
    }

    public static void printAFieldToStringBuilder(StringBuilder sb, String field, Object v, String inden) {
        if (v == null || v.toString().equals("") || v.toString().equals("[]") || v.toString().equals("SEQUENTIAL")
            || v.toString().equals("SHARED_LOCK")) return;

        printlnToStringBuilder(sb, inden + field + ":" + v);
    }

    public static StackTraceElement split = new StackTraceElement("------- one sql exceptions-----", "", "", 0);

    public static TddlException mergeException(List<TddlException> exceptions) {
        // return new OneToManySQLExceptionsWrapper(exceptions);
        TddlException first = exceptions.get(0);
        List<StackTraceElement> stes = new ArrayList<StackTraceElement>(30 * exceptions.size());
        // stes.addAll(Arrays.asList(first.getStackTrace()));
        boolean hasSplit = false;
        for (StackTraceElement ste : first.getStackTrace()) {
            stes.add(ste);
            if (ste == split) {
                hasSplit = true;
            }
        }
        if (!hasSplit) {
            stes.add(split);
        }
        Exception current = first;
        for (int i = 1, n = exceptions.size(); i < n; i++) {

            current = exceptions.get(i);

            hasSplit = false;
            for (StackTraceElement ste : current.getStackTrace()) {
                stes.add(ste);
                if (ste == split) {
                    hasSplit = true;
                }
            }
            if (!hasSplit) {
                stes.add(split);
            }
        }

        first.setStackTrace(stes.toArray(new StackTraceElement[stes.size()]));
        return first;
    }

    public static InputStream getInputStream(String fileName) throws FileNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = GeneralUtil.class.getClassLoader();
        }
        if (fileName.charAt(0) == '/') {
            fileName = fileName.substring(1);
        }
        return classLoader.getResourceAsStream(fileName);
    }

}
