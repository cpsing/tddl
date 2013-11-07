package com.taobao.tddl.atom.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

public class PropLoadTestUtil {

    public static String loadPropFile2String(String classPath) throws IOException {
        String data = null;
        InputStream is = PropLoadTestUtil.class.getClassLoader().getResourceAsStream(classPath);
        Properties prop = new Properties();
        prop.load(is);
        is.close();
        data = convertProp2Str(prop);
        return data;
    }

    public static String convertProp2Str(Properties prop) {
        String data;
        StringBuilder sb = new StringBuilder();
        for (Entry<Object, Object> entry : prop.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            sb.append(key);
            sb.append("=");
            sb.append(value);
            sb.append("\r\n");
        }
        data = sb.toString();
        return data;
    }

    public static Properties loadPropFromFile(String classPath) throws IOException {
        InputStream is = PropLoadTestUtil.class.getClassLoader().getResourceAsStream(classPath);
        Properties prop = new Properties();
        prop.load(is);
        is.close();
        return prop;
    }
}
