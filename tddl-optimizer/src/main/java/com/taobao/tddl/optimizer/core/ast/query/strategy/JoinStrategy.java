package com.taobao.tddl.optimizer.core.ast.query.strategy;

import java.util.Map;

import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * join策略
 * 
 * @author Dreamond
 * @author jianghang 2013-11-14 下午6:17:15
 * @since 5.1.0
 */
public interface JoinStrategy {

    IJoin getQuery(JoinNode joinNode, Map<QueryTreeNode, IQueryTree> queryTreeRootAndItsPlan) throws QueryException;

    JoinType getType();
}
