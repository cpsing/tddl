package com.taobao.tddl.config.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.holder.ConfigHolderFactory;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 使用delegate模式，引入Preheat机制，允许预先构建cache，加速请求处理
 * 
 * @author jianghang 2013-10-28 下午7:36:03
 * @since 5.1.0
 */
public class PreheatDataHandler extends UnitConfigDataHandler {

    private static final Logger   logger = LoggerFactory.getLogger(PreheatDataHandler.class);
    private UnitConfigDataHandler delagate;
    private String                dataId;

    @Override
    public void init(String dataId, List<ConfigDataListener> listenerList, Map<String, Object> prop) {
        this.dataId = dataId;
        delagate.init(dataId, listenerList, prop);
    }

    @Override
    public void init(String dataId, List<ConfigDataListener> configDataListenerList, Map<String, Object> config,
                     String initialData) {
        this.dataId = dataId;
        delagate.init(dataId, configDataListenerList, config, initialData);
    }

    @Override
    public String getData(long timeout, String strategy) {
        if (ConfigHolderFactory.isInit(delagate.getAppName())) {
            String result = ConfigHolderFactory.getConfigDataHolder(delagate.getAppName()).getData(dataId);
            if (!TStringUtil.isEmpty(result)) {
                return result;
            }
            logger.error("PreheatDataHandler Miss Data, Use Default Handler. DataId Is : " + dataId);
        }

        return delagate.getData(timeout, strategy);
    }

    @Override
    public String getNullableData(long timeout, String strategy) {
        if (ConfigHolderFactory.isInit(delagate.getAppName())) {
            return ConfigHolderFactory.getConfigDataHolder(delagate.getAppName()).getData(dataId);
        }

        return delagate.getNullableData(timeout, strategy);
    }

    @Override
    public void addListener(ConfigDataListener configDataListener, Executor executor) {
        delagate.addListener(configDataListener, executor);
    }

    @Override
    public void addListeners(List<ConfigDataListener> configDataListenerList, Executor executor) {
        delagate.addListeners(configDataListenerList, executor);

    }

    @Override
    public void closeUnderManager() {
        delagate.closeUnderManager();
    }

}
