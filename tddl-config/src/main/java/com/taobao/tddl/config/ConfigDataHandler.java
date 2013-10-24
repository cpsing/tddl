package com.taobao.tddl.config;

public interface ConfigDataHandler {

    /**
     * 获取数据，会先从server上取，如果取不到，则使用本地cache.
     * 
     * @param timeout 超时时间
     * @return
     */
    String getData(long timeout);

    /**
     * 添加一个监听器
     * 
     * @param configDataListener
     */
    void addListener(ConfigDataListener configDataListener);
}
