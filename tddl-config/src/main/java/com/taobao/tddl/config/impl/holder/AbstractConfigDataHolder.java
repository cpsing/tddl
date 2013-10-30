package com.taobao.tddl.config.impl.holder;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.utils.extension.ExtensionLoader;

public abstract class AbstractConfigDataHolder implements ConfigDataHolder {

    AbstractConfigDataHolder delegateDataHolder = null;

    public abstract void init();

    protected void loadDelegateExtension() {
        this.delegateDataHolder = ExtensionLoader.load(AbstractConfigDataHolder.class);
    }

    @Override
    public String getData(String dataId) {
        return delegateDataHolder.getData(dataId);
    }

    @Override
    public Map<String, String> getData(List<String> dataIds) {
        return delegateDataHolder.getData(dataIds);
    }

    protected Map<String, String> queryAndHold(List<String> dataIds, String unitName) {
        return delegateDataHolder.queryAndHold(dataIds, unitName);
    }

}
