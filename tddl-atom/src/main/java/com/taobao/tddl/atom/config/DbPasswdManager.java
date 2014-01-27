package com.taobao.tddl.atom.config;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.config.ConfigDataListener;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午5:55:44
 * @since 5.0.0
 */
public interface DbPasswdManager {

    /**
     * 获取数据库密码
     * 
     * @return
     */
    public String getPasswd();

    /**
     * 注册应用配置监听
     * 
     * @param Listener
     */
    public void registerPasswdConfListener(ConfigDataListener Listener);

    /**
     * 停止DbPasswdManager
     */
    public void stopDbPasswdManager() throws TddlException;
}
