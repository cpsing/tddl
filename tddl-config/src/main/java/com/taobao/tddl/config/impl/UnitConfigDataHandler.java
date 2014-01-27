package com.taobao.tddl.config.impl;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;

/**
 * 基于unit的{@linkplain ConfigDataHandler}的delagate实现
 * 
 * @author jianghang 2013-10-28 下午7:00:35
 * @since 5.0.0
 */
public abstract class UnitConfigDataHandler extends AbstractLifecycle implements ConfigDataHandler {

    protected String                   unitName;
    protected String                   appName;
    protected String                   dataId;
    protected Map<String, Object>      config;
    protected List<ConfigDataListener> listeners = Lists.newArrayList();
    protected String                   initialData;

    protected UnitConfigDataHandler loadHandlerExtension() {
        UnitConfigDataHandler unitHandler = ExtensionLoader.load(UnitConfigDataHandler.class);
        unitHandler.setAppName(appName);
        unitHandler.setUnitName(unitName);
        unitHandler.setDataId(dataId);
        unitHandler.setInitialData(initialData);
        unitHandler.setConfig(config);
        unitHandler.setListeners(listeners);
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

    public String getDataId() {
        return dataId;
    }

    public void setDataId(String dataId) {
        this.dataId = dataId;
    }

    public List<ConfigDataListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<ConfigDataListener> listeners) {
        this.listeners = listeners;
    }

    public void addListener(ConfigDataListener listener) {
        this.listeners.add(listener);
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> props) {
        this.config = props;
    }

    public String getInitialData() {
        return initialData;
    }

    public void setInitialData(String initialData) {
        this.initialData = initialData;
    }

}
