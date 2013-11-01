package com.taobao.tddl.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 获取配置的处理器
 * 
 * @author <a href="zylicfc@gmail.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11上午11:22:29
 */
public interface ConfigDataHandler {

    public static final String FIRST_SERVER_STRATEGY            = "firstServer";
    public static final String FIRST_CACHE_THEN_SERVER_STRATEGY = "firstCache";
    public static final long   GET_DATA_TIMEOUT                 = 10 * 1000;

    /**
     * DefaultConfigDataHandler会在 实例化具体的Handler之后调用此方法 给予Handler相关信息
     * 
     * @param dataId 数据在配置平台上注册的id
     * @param listenerList 数据监听器列表
     * @param prop 全局配置和运行时
     */
    void init(String dataId, List<ConfigDataListener> listenerList, Map<String, Object> prop);

    /**
     * 允许指定initialData进行初始化
     * 
     * @param dataId
     * @param configDataListenerList
     * @param config
     * @param unitName
     * @param initialData
     */
    void init(final String dataId, final List<ConfigDataListener> configDataListenerList,
              final Map<String, Object> config, String initialData);

    /**
     * 从配置中心拉取数据
     * 
     * @param timeout 获取配置信息超时时间
     * @param strategy 获取配置策略
     * @return
     */
    String getData(long timeout, String strategy);

    /**
     * 从配置中心拉取数据，返回结果允许为null
     * 
     * @param timeout 获取配置信息超时时间
     * @param strategy 获取配置策略
     * @return
     */
    String getNullableData(long timeout, String strategy);

    /**
     * 为推送过来的数据注册处理的监听器
     * 
     * @param configDataListener 监听器
     * @param executor 执行的executor
     */
    void addListener(ConfigDataListener configDataListener, Executor executor);

    /**
     * 为推送过来的数据注册多个处理监听器
     * 
     * @param configDataListenerList 监听器列表
     * @param executor 执行的executor
     */
    void addListeners(List<ConfigDataListener> configDataListenerList, Executor executor);

    /**
     * 停止底层配置管理器
     */
    void closeUnderManager();
}
