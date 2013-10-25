package com.taobao.tddl.config;

/**
 * @author wuxie.wl 获取配置
 * com.taobao.and_orV0.[ANDOR_ONLY_SPEC_APP_NAME_(中括弧内是APPname)][Minus(是dataID)]
 * @author whisper
 */
public interface ConfigDataHandlerFactory {

    public static final String configure_version = "V0";
    public static final String configure_prefix  = "com.taobao.and_or" + configure_version + ".";

    // configure_prefix+APPNAME+....
    /**
     * 获取配置的主要方法
     * 
     * @param dataId 数据对应id
     * @param configDataListener 如果有变更则获取通知
     * @return 处理机，也可以处理数据
     */
    ConfigDataHandler getConfigDataHandler(String appName, String dataId, ConfigDataListener configDataListener);

    ConfigDataHandler getConfigDataHandler(String dataId, ConfigDataListener configDataListener);
}
