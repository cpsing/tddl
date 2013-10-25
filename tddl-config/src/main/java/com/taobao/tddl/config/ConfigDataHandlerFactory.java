package com.taobao.tddl.config;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * @author whisper
 * @author <a href="zylicfc@gmail.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11上午11:22:29
 * @desc 得到具体的配置处理器实例
 */
public interface ConfigDataHandlerFactory {

    /**
     * 对某一个dataId进行监听
     * 
     * @param dataId 数据在配置中心注册的id
     * @return 返回配置数据处理器实例
     */
    ConfigDataHandler getConfigDataHandler(String dataId, String unitName);

    /**
     * 对某一个dataId进行监听，使用者提供回调监听器
     * 
     * @param dataId 数据在p诶值中心注册的id
     * @param configDataListener 数据回调监听器
     * @return 返回配置数据处理器实例
     */
    ConfigDataHandler getConfigDataHandlerWithListener(String dataId, ConfigDataListener configDataListener,
                                                       String unitName);

    /**
     * 对某一个dataId进行监听，使用者提供回调监听器列表， 并且提供执行线程池和内部一些配置(可能被handler忽视)
     * 
     * @param dataId 数据在配置中心注册的id
     * @param configDataListenerList 数据回调监听器列表
     * @param executor 数据接收处理线程池
     * @param config TDDL内部对handler提供的一些配置
     * @return 返回配置数据处理器实例
     */
    ConfigDataHandler getConfigDataHandlerWithFullConfig(String dataId,
                                                         List<ConfigDataListener> configDataListenerList,
                                                         Executor executor, Map<String, String> config, String unitName);
}
