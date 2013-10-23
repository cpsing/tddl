package com.taobao.tddl.common.model.lifecycle;

/**
 * lifecycle状态
 * 
 * @author jianghang 2013-10-23 下午5:20:21
 * @since 5.1.0
 */
public enum LifecycleState {

    /**
     * 正常运行
     */
    RUNNING,
    /**
     * 正常关闭状态
     */
    STOPPED,

    /**
     * 异常关闭状态
     */
    ABORTED;

    public boolean isRunning() {
        return RUNNING == this;
    }

    public boolean isStopped() {
        return STOPPED == this;
    }

    public boolean isAborted() {
        return ABORTED == this;
    }
}
