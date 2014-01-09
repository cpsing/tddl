package com.taobao.tddl.config.impl;

import java.util.Map;

import com.taobao.tddl.config.ConfigDataHandlerFactory;

public class ConfigDataHandlerCity {

    public static ConfigDataHandlerFactory getFactory(String appName, String unitName) {
        if (appName == null || appName.trim().isEmpty()) {
            return getSimpleFactory();
        }
        return getPreHeatFactory(appName, unitName);
    }

    /**
     * @param appName
     * @param unitName
     * @param localValues 从本地配置文件里读出来的值，优先使用这里面的
     * @return
     */
    public static ConfigDataHandlerFactory getFactory(String appName, String unitName, Map<String, String> localValues) {
        if (localValues == null) {
            return getFactory(appName, unitName);
        }
        return new LocalFirstConfigDataHandlerFactory(getFactory(appName, unitName), localValues);

    }

    public static ConfigDataHandlerFactory getSimpleFactory() {
        return new UnitConfigDataHandlerFactory();
    }

    public static ConfigDataHandlerFactory getPreHeatFactory(String appName, String unitName) {
        return new UnitConfigDataHandlerFactory(unitName, appName);
    }

    public static ConfigDataHandlerFactory getFileFactory(String appName) {
        return new FileConfigDataHandlerFactory("", appName, null);
    }

}
