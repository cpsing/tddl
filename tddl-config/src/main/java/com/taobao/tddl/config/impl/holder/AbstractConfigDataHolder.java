package com.taobao.tddl.config.impl.holder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

public abstract class AbstractConfigDataHolder extends AbstractLifecycle implements ConfigDataHolder {

    protected Map<String, String>      configHouse         = new HashMap<String, String>();
    protected AbstractConfigDataHolder delegateDataHolder  = null;
    protected ConfigDataHolder         sonConfigDataHolder = null;

    protected void loadDelegateExtension() {
        this.delegateDataHolder = ExtensionLoader.load(AbstractConfigDataHolder.class);
        this.delegateDataHolder.sonConfigDataHolder = sonConfigDataHolder;
    }

    @Override
    public String getData(String dataId) {
        return configHouse.containsKey(dataId) ? configHouse.get(dataId) : delegateDataHolder.getData(dataId);
    }

    @Override
    public Map<String, String> getData(List<String> dataIds) {
        Map<String, String> result = new HashMap<String, String>();
        for (String dataId : dataIds) {
            result.put(dataId, getData(dataId));
        }
        return result;
    }

    protected Map<String, String> queryAndHold(List<String> dataIds, String unitName) {
        if (dataIds.isEmpty()) {
            return Maps.newHashMap();
        }

        return delegateDataHolder.queryAndHold(dataIds, unitName);
    }

    protected void addDatas(Map<String, String> confMap) {
        configHouse.putAll(confMap);
    }

    protected String getDataFromSonHolder(String dataId) {
        return sonConfigDataHolder == null ? null : sonConfigDataHolder.getData(dataId);
    }

    public void setSonConfigDataHolder(ConfigDataHolder sonConfigDataHolder) {
        this.sonConfigDataHolder = sonConfigDataHolder;
    }

}
