package com.taobao.tddl.optimizer.core.plan.query;

/**
 * @author jianghang 2013-11-8 下午2:57:14
 * @since 5.1.0
 */
public interface IJoin extends IParallelizableQueryTree<IJoin> {

    public enum JoinType {
        HASH_JOIN, NEST_LOOP_JOIN, INDEX_NEST_LOOP, SORT_MERGE_JOIN;
    }
}
