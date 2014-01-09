package com.taobao.tddl.qatest.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author zhuoxue.yll
 */
public class LoadPropsUtil {

    private static final Log logger = LogFactory.getLog(LoadPropsUtil.class);

    /**
     * load the file to Properties
     * 
     * @param classPath
     * @return
     */
    public static Properties loadProps(String classPath) {
        Properties prop = new Properties();
        InputStream is = LoadPropsUtil.class.getClassLoader().getResourceAsStream(classPath);
        try {
            prop.load(is);
        } catch (IOException e) {
            logger.error(classPath + " file is not exist!");
        } finally {
            IOUtils.closeQuietly(is);
        }
        return prop;
    }

    public static String loadProps2Str(String classPath) {
        Properties prop = loadProps(classPath);
        return convertProp2Str(prop);
    }

    public static String loadProps2OneLine(String classPath, String aKey) throws Exception {
        Properties prop = loadProps(classPath);
        StringBuilder sb = new StringBuilder();
        for (Entry<Object, Object> entry : prop.entrySet()) {
            String key = (String) entry.getKey();
            if (key.equalsIgnoreCase(aKey)) {
                String value = (String) entry.getValue();
                sb.append(value);
            }
        }
        return sb.toString();
    }

    public static String convertProp2Str(Properties prop) {
        StringBuilder sb = new StringBuilder();
        for (Entry<Object, Object> entry : prop.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            sb.append(key);
            sb.append("=");
            sb.append(value);
            sb.append("\r\n");
        }
        return sb.toString();
    }
}
