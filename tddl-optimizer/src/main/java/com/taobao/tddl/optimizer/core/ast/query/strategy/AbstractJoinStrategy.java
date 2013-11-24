package com.taobao.tddl.optimizer.core.ast.query.strategy;

import java.util.ArrayList;
import java.util.Map;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * @since 5.1.0
 */
public abstract class AbstractJoinStrategy implements JoinStrategy {

    public IJoin getQuery(JoinNode joinNode, Map<QueryTreeNode, IQueryTree> queryTreeRootAndItsPlan)
                                                                                                    throws QueryException {
        if (queryTreeRootAndItsPlan != null && queryTreeRootAndItsPlan.containsKey(joinNode)) {
            return (IJoin) queryTreeRootAndItsPlan.get(joinNode);
        }

        IJoin join = ASTNodeFactory.getInstance().createJoin();
        join.setRightNode(joinNode.getRightNode().toDataNodeExecutor());
        join.setLeftNode(joinNode.getLeftNode().toDataNodeExecutor());
        join.setJoinType(this.getType()).setLeftOuter(joinNode.getLeftOuter()).setRightOuter(joinNode.getRightOuter());

        join.setJoinOnColumns((joinNode.getLeftKeys()), (joinNode.getRightKeys()));
        join.setOrderBys(joinNode.getOrderBys());
        join.setLimitFrom(joinNode.getLimitFrom()).setLimitTo(joinNode.getLimitTo());
        join.executeOn(joinNode.getDataNode()).setConsistent(true);
        join.setValueFilter(joinNode.getResultFilter());
        join.having(joinNode.getHavingFilter());
        join.setAlias(joinNode.getAlias());
        join.setGroupBys(joinNode.getGroupBys());
        join.setIsSubQuery(joinNode.isSubQuery());
        join.setOtherJoinOnFilter(joinNode.getOtherJoinOnFilter());
        if (joinNode.isCrossJoin()) {
            join.setColumns(new ArrayList(0));
        } else {
            join.setColumns((joinNode.getColumnsSelected()));
        }

        join.setWhereFilter(joinNode.getAllWhereFilter());
        return join;
    }

    public String toString() {
        return getType().toString();
    }

}
