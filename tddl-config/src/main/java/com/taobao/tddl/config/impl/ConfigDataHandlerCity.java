package com.taobao.tddl.config.impl;

import com.taobao.tddl.config.ConfigDataHandlerFactory;

public class ConfigDataHandlerCity {

    public static ConfigDataHandlerFactory getFactory(String appName, String unitName) {
        if (appName == null || appName.trim().isEmpty()) {
            return getSimpleFactory();
        }
        return getPreHeatFactory(appName, unitName);
    }

    public static ConfigDataHandlerFactory getSimpleFactory() {
        return new UnitConfigDataHandlerFactory();
    }

    public static ConfigDataHandlerFactory getPreHeatFactory(String appName, String unitName) {
        return new UnitConfigDataHandlerFactory(unitName, appName);
    }

}
