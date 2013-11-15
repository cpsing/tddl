package com.taobao.tddl.optimizer.core.ast.query.strategy;

import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;

/**
 * @since 5.1.0
 */
public class SortMergeJoin extends AbstractJoinStrategy {

    public JoinType getType() {
        return JoinType.SORT_MERGE_JOIN;
    }

}
