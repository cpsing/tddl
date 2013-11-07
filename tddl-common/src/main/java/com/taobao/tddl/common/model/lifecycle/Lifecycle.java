package com.taobao.tddl.common.model.lifecycle;

/**
 * @author jianghang 2013-10-23 下午5:19:58
 * @since 5.1.0
 */
public interface Lifecycle {

    /**
     * 正常启动
     */
    void init();

    /**
     * 正常停止
     */
    void destory();

    /**
     * 是否存储运行运行状态
     * 
     * @return
     */
    boolean isInited();

}
