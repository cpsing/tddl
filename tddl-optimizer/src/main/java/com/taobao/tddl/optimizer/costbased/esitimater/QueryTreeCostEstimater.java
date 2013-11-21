package com.taobao.tddl.optimizer.costbased.esitimater;

import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;

/**
 * @author Dreamond
 */
public interface QueryTreeCostEstimater {

    /**
     * 根据查询树，预估一下执行代价，如果统计状态服务不可用，抛出异常
     * 
     * @param query
     * @return
     * @throws StatisticsUnavailableException
     */
    Cost estimate(QueryTreeNode query) throws StatisticsUnavailableException;
}
