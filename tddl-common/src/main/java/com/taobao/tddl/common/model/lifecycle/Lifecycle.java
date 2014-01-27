package com.taobao.tddl.common.model.lifecycle;

import com.taobao.tddl.common.exception.TddlException;

/**
 * @author jianghang 2013-10-23 下午5:19:58
 * @since 5.0.0
 */
public interface Lifecycle {

    /**
     * 正常启动
     */
    void init() throws TddlException;

    /**
     * 正常停止
     */
    void destory() throws TddlException;

    /**
     * 是否存储运行运行状态
     * 
     * @return
     */
    boolean isInited() throws TddlException;

}
