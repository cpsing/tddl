package com.taobao.tddl.config.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

public class LocalFirstConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    ConfigDataHandlerFactory    delegateFactory = null;
    private Map<String, String> localValues     = null;

    public LocalFirstConfigDataHandlerFactory(ConfigDataHandlerFactory delegateFactory, Map<String, String> localValues){
        this.delegateFactory = delegateFactory;
        this.localValues = localValues;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId) {
        if (localValues != null && localValues.containsKey(dataId)) {
            return new StaticConfigDataHandler(localValues.get(dataId));
        }

        return delegateFactory.getConfigDataHandler(dataId);
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, ConfigDataListener configDataListener) {
        if (localValues != null && localValues.containsKey(dataId)) {
            return new StaticConfigDataHandler(localValues.get(dataId));
        }

        return delegateFactory.getConfigDataHandler(dataId, configDataListener);
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, List<ConfigDataListener> configDataListenerList,
                                                  Executor executor, Map<String, Object> config) {
        if (localValues != null && localValues.containsKey(dataId)) {
            return new StaticConfigDataHandler(localValues.get(dataId));
        }

        return delegateFactory.getConfigDataHandler(dataId, configDataListenerList, executor, config);
    }

}
