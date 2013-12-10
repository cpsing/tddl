package com.taobao.tddl.config.impl;

import java.util.List;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;

public class StaticConfigDataHandler extends AbstractLifecycle implements ConfigDataHandler {

    String value;

    public StaticConfigDataHandler(String value){
        this.value = value;
    }

    @Override
    public String getData(long timeout, String strategy) {
        return value;
    }

    @Override
    public String getNullableData(long timeout, String strategy) {
        return value;
    }

    @Override
    public void addListener(ConfigDataListener configDataListener, Executor executor) {

    }

    @Override
    public void addListeners(List<ConfigDataListener> configDataListenerList, Executor executor) {

    }

}
