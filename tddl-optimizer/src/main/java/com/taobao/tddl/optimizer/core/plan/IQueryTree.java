package com.taobao.tddl.optimizer.core.plan;

/**
 * @author jianghang 2013-11-8 下午2:54:32
 * @since 5.1.0
 */
public interface IQueryTree<RT extends IQueryTree> extends IDataNodeExecutor<RT> {

    public enum LOCK_MODEL {
        SHARED_LOCK, EXCLUSIVE_LOCK;
    }
}
