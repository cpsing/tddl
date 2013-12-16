package com.taobao.tddl.optimizer.costbased.esitimater;

import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;

/**
 * @author Dreamond
 */
public class JoinNodeCostEstimater implements QueryTreeCostEstimater {

    public Cost estimate(QueryTreeNode query) throws StatisticsUnavailableException {
        JoinNode join = (JoinNode) query;

        Cost leftCost = CostEsitimaterFactory.estimate(join.getLeftNode());
        Cost rightCost = CostEsitimaterFactory.estimate(join.getRightNode());
        Cost cost = new Cost();
        cost.setIsOnFly(true);
        long rowCount = 0;
        long networkCost = 0;
        long scanRowCount = 0;
        boolean isOnFly = true;

        // 估算IO开销
        if (rightCost.isOnFly()) {
            cost.setDiskIO(leftCost.getDiskIO() + rightCost.getDiskIO());
        } else {
            cost.setDiskIO(leftCost.getDiskIO() + leftCost.getRowCount());
        }
        // 估算行数
        // 现在假定join后的行数为较小表的行数，这个假设对于大多数场景都是成立的
        if (leftCost.getRowCount() < rightCost.getRowCount()) {
            rowCount = (leftCost.getRowCount());
        } else {
            rowCount = (rightCost.getRowCount());
        }

        // 估算网络开销
        if (join.getDataNode() != null) {
            if (!join.getDataNode().equals(join.getLeftNode().getDataNode())) {
                networkCost += leftCost.getRowCount();
            }
            if (!join.getDataNode().equals(join.getRightNode().getDataNode())) {
                networkCost += rightCost.getRowCount();
            }
        }

        // 如果是IndexNestLoop，则数据还在磁盘上
        if (join.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP) {
            cost.setDiskIO(leftCost.getRowCount());
            isOnFly = false;
            scanRowCount = leftCost.getScanCount() + rightCost.getScanCount();
        } else {
            // sort merge
            // 右边的先要拿出数据
            // 再要将符合的数据插到临时表
            // 所以等于是两次
            scanRowCount = leftCost.getScanCount() + rightCost.getScanCount() * 2 + rightCost.getRowCount();
        }

        if (query.getLimitFrom() != null
            && (query.getLimitFrom() instanceof Long || query.getLimitFrom() instanceof Long)
            && (Long) query.getLimitFrom() != 0 && query.getLimitTo() != null
            && (query.getLimitTo() instanceof Long || query.getLimitTo() instanceof Long)
            && (Long) query.getLimitTo() != 0) {
            rowCount = ((Long) query.getLimitTo() - (Long) query.getLimitFrom());
        } else if (query.getLimitFrom() != null || query.getLimitTo() != null) {
            rowCount = rowCount / 2;
        }

        cost.setNetworkCost(networkCost);
        cost.setRowCount(rowCount);
        cost.setIsOnFly(isOnFly);
        cost.setScanCount(scanRowCount);
        return cost;
    }
}
