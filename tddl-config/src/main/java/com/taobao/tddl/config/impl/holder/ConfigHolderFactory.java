package com.taobao.tddl.config.impl.holder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 简单的{@linkplain ConfigDataHolder}管理
 */
public class ConfigHolderFactory {

    private static Map<String, ConfigDataHolder> holderMap = new ConcurrentHashMap<String, ConfigDataHolder>();

    public static ConfigDataHolder getConfigDataHolder(String appName) {
        return holderMap.get(appName);
    }

    public static void addConfigDataHolder(String appName, ConfigDataHolder configDataHolder) {
        holderMap.put(appName, configDataHolder);
    }

    public static void removeConfigHoder(String appName) {
        holderMap.remove(appName);
    }

    public static boolean isInit(String appName) {
        return appName != null && holderMap.containsKey(appName);
    }

}
