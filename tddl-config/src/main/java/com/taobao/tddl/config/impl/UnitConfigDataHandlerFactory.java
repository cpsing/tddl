package com.taobao.tddl.config.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

/**
 * 基于unit的data handler factory实现，通过Extension机制获取对应的
 * {@linkplain ConfigDataHandler}
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11下午01:17:21
 */
public class UnitConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    public static final String DEFAULT_UNITNAME = "";              // 空值
    protected String           unitName         = DEFAULT_UNITNAME;
    protected String           appName;                            // 如果为null，则不会进入holder处理逻辑

    public UnitConfigDataHandlerFactory(){
    }

    public UnitConfigDataHandlerFactory(String unitName){
        this.unitName = unitName;
    }

    public UnitConfigDataHandlerFactory(String unitName, String appName){
        this.unitName = unitName;
        this.appName = appName;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId) {
        return this.getConfigDataHandler(dataId, null);
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, ConfigDataListener configDataListener) {
        List<ConfigDataListener> configDataListenerList = new ArrayList<ConfigDataListener>();
        configDataListenerList.add(configDataListener);
        return this.getConfigDataHandler(dataId, configDataListenerList, null, new HashMap<String, Object>());
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, List<ConfigDataListener> configDataListenerList,
                                                  Executor executor, Map<String, Object> config) {
        // 获取config data handler的扩展实现
        PreheatDataHandler instance = new PreheatDataHandler();
        instance.setUnitName(unitName);
        instance.setAppName(appName);
        instance.setDataId(dataId);
        instance.setListeners(clearNullListener(configDataListenerList));
        instance.setConfig(config);
        try {
            instance.init();// 启动
        } catch (TddlException e) {
            throw new TddlRuntimeException(e);
        }
        return instance;
    }

    /**
     * 不能更换listenerList的引用，避免后续对list的修改无效
     * 
     * <pre>
     * 以下做法是被禁止的 
     * List result = new List
     *  result.add 
     *  return result
     * </pre>
     */
    protected List<ConfigDataListener> clearNullListener(List<ConfigDataListener> configDataListenerList) {
        for (int index = 0; index < configDataListenerList.size(); index++) {
            configDataListenerList.remove(null);
        }
        return configDataListenerList;
    }

}
