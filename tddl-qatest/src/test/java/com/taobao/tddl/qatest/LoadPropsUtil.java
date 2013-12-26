package com.taobao.tddl.qatest;


import java.io.IOException;
import java.io.InputStream;
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
    public static Properties loadProps2Str(String classPath) {
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

}
