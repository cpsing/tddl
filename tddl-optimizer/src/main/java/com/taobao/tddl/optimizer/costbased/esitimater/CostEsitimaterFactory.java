package com.taobao.tddl.optimizer.costbased.esitimater;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
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

    public static Cost estimater(QueryTreeNode query) throws StatisticsUnavailableException {
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
}
