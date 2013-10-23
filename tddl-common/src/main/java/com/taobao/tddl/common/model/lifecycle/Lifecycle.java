package com.taobao.tddl.common.model.lifecycle;

/**
 * @author jianghang 2013-10-23 下午5:19:58
 * @since 5.1.0
 */
public interface Lifecycle extends Abortable {

    /**
     * 正常启动
     */
    void start();

    /**
     * 正常停止
     */
    void stop();

    void stop(String why);

    /**
     * 是否存储运行运行状态
     * 
     * @return
     */
    boolean isRunning();

    /**
     * 是否处于停止状态，有可能是正常停止，也有可能是异常停止
     * 
     * @return
     */
    boolean isStopped();

}
