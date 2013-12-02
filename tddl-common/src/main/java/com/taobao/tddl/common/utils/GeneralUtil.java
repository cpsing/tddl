package com.taobao.tddl.common.utils;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * 公共方便方法
 * 
 * @author whisper
 */
public class GeneralUtil {

    static Pattern pattern = Pattern.compile("\\d+$");

    /**
     * 如果为空抛出IllegalArgumentException
     * 
     * @param objects
     */
    public static void assertNotNull(String varName, Object... objects) {
        if (objects == null || varName == null) {
            throw new IllegalArgumentException("var should not be null");
        }
        int index = 0;
        for (Object obj : objects) {
            if (obj == null) {
                throw new IllegalArgumentException(getColumnName(index, varName) + ". var should not be null");
            }
            index++;
        }
    }

    public static void assertStringNotEmptyAndNull(String varName, String... strings) {
        if (strings == null || strings.length == 0 || varName == null) {
            throw new IllegalArgumentException("var should not be null or empty");
        }
        int index = 0;
        for (String str : strings) {
            if (str == null || str.length() == 0) {
                throw new IllegalArgumentException(getColumnName(index, varName) + " var should not be null or empty");
            }
            index++;
        }
    }

    public static String getColumnName(int index, String columns) {
        String[] strs = columns.split(",");
        if (index >= strs.length) {
            return "error column name";
        }
        return strs[index];

    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.size() == 0;
    }

    public static boolean isNotEmpty(Collection collection) {
        return collection != null && collection.size() != 0;
    }

    public static String getLogicTableName(String indexName) {
        if (indexName == null) {
            return null;
        }
        int index = indexName.indexOf(".");
        if (index != -1) {
            return indexName.substring(0, index);
        } else {
            return indexName;
        }
    }

    public static String getRealTableName(String indexName) {
        if (indexName == null) {
            return null;
        }
        if (indexName.contains(".")) {
            StringBuilder tableName = new StringBuilder();
            String[] tmp = StringUtils.split(indexName, ".");
            tableName.append(tmp[0]);

            Matcher matcher = pattern.matcher(tmp[1]);
            if (matcher.find()) {
                tableName.append("_").append(matcher.group());
            }
            return tableName.toString();
        } else {
            return indexName;
        }
    }

    public static String getTab(int count) {
        StringBuffer tab = new StringBuffer();
        for (int i = 0; i < count; i++)
            tab.append("    ");
        return tab.toString();
    }

    public static String getExtraCmd(Map<String, Comparable> extraCmd, String key) {
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

    public static void checkInterrupted() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    public static void printlnToStringBuilder(StringBuilder sb, String v) {
        sb.append(v).append("\n");
    }
}
