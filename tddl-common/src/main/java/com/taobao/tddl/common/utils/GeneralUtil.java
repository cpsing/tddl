package com.taobao.tddl.common.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlRuntimeException;

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

    public static InputStream getInputStream(String fileName) throws FileNotFoundException {

        String rootClassPath = GeneralUtil.class.getResource("/").getPath();
        String tempClassFileName = null;
        if (fileName.startsWith("/")) {
            // root class path 结尾以"/" 结尾，所以去掉一个/
            tempClassFileName = rootClassPath + fileName.substring(1);
        } else {
            tempClassFileName = rootClassPath + fileName;
        }
        InputStream in = getInputStreamInner(tempClassFileName);
        if (in != null) {
            return in;
        }
        return getInputStreamInner(fileName);
    }

    private static InputStream getInputStreamInner(String fileName) throws FileNotFoundException {
        URL url = GeneralUtil.class.getClassLoader().getResource(fileName);
        String path = "";
        if (url != null) {
            path = url.getFile();
            if (new File(path).isDirectory()) {
                throw new IllegalArgumentException("file is directory." + path);
            }
        }
        InputStream in;
        File file = new File(fileName);
        if (!file.exists()) {
            return null;
        }
        if (file.isDirectory()) {
            throw new IllegalArgumentException("file is directory" + path);
        }
        in = new FileInputStream(file);
        return in;
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

    public static boolean isEqual(Object o1, Object o2) {
        if (o1 == o2) return true;

        return o1 == null ? o2.equals(o1) : o1.equals(o2);
    }

    public static String replaceBlank(String str) {

        String dest = "";

        if (str != null) {

            Pattern p = Pattern.compile("\t|\r|\n");

            Matcher m = p.matcher(str);

            dest = m.replaceAll("");

        }

        return dest;

    }

    public static Properties parseProperties(Object data, String msg) {
        Properties p = null;
        if (data == null) {
            return null;
        } else if (data instanceof Properties) {
            p = (Properties) data;
        } else if (data instanceof String) {
            p = new Properties();
            try {
                p.load(new ByteArrayInputStream(((String) data).getBytes()));
            } catch (IOException e) {
                throw new TddlRuntimeException(e);
            }
        } else {
            throw new TddlRuntimeException("unsupport data parse : " + data);
        }

        return p;
    }

}
