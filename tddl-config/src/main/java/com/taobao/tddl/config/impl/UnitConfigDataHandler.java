package com.taobao.tddl.config.impl;

import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.config.ConfigDataHandler;

/**
 * 基于unit的{@linkplain ConfigDataHandler}的delagate实现
 * 
 * @author jianghang 2013-10-28 下午7:00:35
 * @since 5.1.0
 */
public abstract class UnitConfigDataHandler implements ConfigDataHandler {

    protected String unitName;
    protected String appName;

    protected ConfigDataHandler loadHandlerExtension() {
        UnitConfigDataHandler unitHandler = ExtensionLoader.load(UnitConfigDataHandler.class);
        unitHandler.setUnitName(unitName);
        return unitHandler;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

}
