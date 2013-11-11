package com.taobao.tddl.optimizer.core.plan.query;

import com.taobao.tddl.optimizer.core.plan.IQueryTree;


/**
 * @author jianghang 2013-11-8 下午2:54:28
 * @since 5.1.0
 */
public interface IParallelizableQueryTree<RT extends IParallelizableQueryTree> extends IQueryTree<RT> {

    /**
     * 是否并行
     */
    public enum QUERY_CONCURRENCY {
        SEQUENTIAL, CONCURRENT;
    }

    public RT setQueryConcurrency(QUERY_CONCURRENCY queryConcurrency);

    public QUERY_CONCURRENCY getQueryConcurrency();
}
