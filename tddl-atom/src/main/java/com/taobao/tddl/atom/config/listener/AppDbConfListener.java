package com.taobao.tddl.atom.config.listener;

/**
 * 应用配置变化监听器
 * 
 * @author qihao
 */
public interface AppDbConfListener {

    void handleData(String dataId, String data);
}
