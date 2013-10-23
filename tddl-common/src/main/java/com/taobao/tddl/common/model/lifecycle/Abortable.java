package com.taobao.tddl.common.model.lifecycle;

/**
 * @author jianghang 2013-10-23 下午5:19:30
 * @since 5.1.0
 */
public interface Abortable {

    /**
     * 异常中断停止
     * 
     * @param why
     * @param e
     */
    public void abort(String why, Throwable e);

    /**
     * 是否处于异常停止状态。<br/>
     * 如果 lifecycle 是被正常 stop ，而不是被 abort 的，则返回 false
     * 
     * @return 当被 abort 之后，才会返回 true
     */
    public boolean isAborted();

}
