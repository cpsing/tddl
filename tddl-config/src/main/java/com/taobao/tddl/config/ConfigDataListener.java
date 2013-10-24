package com.taobao.tddl.config;

/**
 * @author shenxun
 */
public interface ConfigDataListener {

    /**
     * 变更后，系统会调用这个方法传递数据给你
     * 
     * @param data
     */
    void onDataRecieved(String data);
}
