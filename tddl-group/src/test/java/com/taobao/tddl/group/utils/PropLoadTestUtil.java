package com.taobao.tddl.group.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class PropLoadTestUtil {

    public static String loadPropFile2String(String classPath) {
        String data = null;
        InputStream is = PropLoadTestUtil.class.getClassLoader().getResourceAsStream(classPath);
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            throw new TddlRuntimeException(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
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

    public static Properties loadPropFromFile(String classPath) {
        InputStream is = PropLoadTestUtil.class.getClassLoader().getResourceAsStream(classPath);
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            throw new TddlRuntimeException(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
        return prop;
    }
}
