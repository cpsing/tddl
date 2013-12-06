package com.taobao.tddl.optimizer.costbased.esitimater;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;

/**
 * 计算结构树的cost
 * 
 * @since 5.1.0
 */
public class CostEsitimaterFactory {

    private static JoinNodeCostEstimater  joinNodeCostEstimater  = new JoinNodeCostEstimater();
    private static MergeNodeCostEstimater mergeNodeCostEstimater = new MergeNodeCostEstimater();
    private static QueryNodeCostEstimater queryNodeCostEstimater = new QueryNodeCostEstimater();

    public static Cost estimate(QueryTreeNode query) throws StatisticsUnavailableException {
        if (query instanceof JoinNode) {
            return joinNodeCostEstimater.estimate(query);
        } else if (query instanceof MergeNode) {
            return mergeNodeCostEstimater.estimate(query);
        } else if (query instanceof QueryNode || query instanceof TableNode) {
            return queryNodeCostEstimater.estimate(query);
        } else {
            throw new NotSupportException();
        }

    }

    /**
     * 参考derby数据库实现
     */
    public static double selectivity(OPERATION operator) {
        if (operator == OPERATION.EQ) {
            return 0.1;
        }
        if (operator == OPERATION.GT || operator == OPERATION.GT_EQ) {
            return 0.33;
        }

        if (operator == OPERATION.LT || operator == OPERATION.LT_EQ) {
            return 0.33;
        }

        if (operator == OPERATION.NOT_EQ) {
            return 0.9;
        }

        if (operator == OPERATION.IS_NULL) {
            return 0.1;
        }

        if (operator == OPERATION.IS_NOT_NULL) {
            return 0.9;
        }

        if (operator == OPERATION.LIKE) {
            return 0.9;
        }
        if (operator == OPERATION.IN) {
            return 0.2;
        }

        return 0.5;
    }
}
