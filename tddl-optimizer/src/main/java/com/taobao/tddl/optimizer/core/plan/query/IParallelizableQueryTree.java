package com.taobao.tddl.optimizer.core.plan.query;

import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * @since 5.0.0
 */
public interface IParallelizableQueryTree<RT extends IQueryTree> extends IQueryTree<RT> {

    /**
     * 是否并行
     */
    public enum QUERY_CONCURRENCY {
        SEQUENTIAL, CONCURRENT;
    }

    public RT setQueryConcurrency(QUERY_CONCURRENCY queryConcurrency);

    public QUERY_CONCURRENCY getQueryConcurrency();
}
