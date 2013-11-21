package com.taobao.tddl.optimizer.costbased.esitimater;

import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.costbased.Cost;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;

/**
 * @author Dreamond
 */
public interface QueryTreeCostEstimater {

    Cost estimate(QueryTreeNode query) throws StatisticsUnavailableException;
}
