package com.taobao.tddl.optimizer.costbased.esitimater;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;

/**
 * @author Dreamond
 */
public class MergeNodeCostEstimater implements QueryTreeCostEstimater {

    @Override
    public Cost estimate(QueryTreeNode query) throws StatisticsUnavailableException {
        MergeNode merge = (MergeNode) query;
        Cost cost = new Cost();

        long rowCount = 0;
        long io = 0;
        long networkCost = 0;
        long scanRowCount = 0;
        if (!(merge.getChildren().get(0) instanceof QueryTreeNode)) {
            return cost;
        }

        for (ASTNode sub : merge.getChildren()) {
            Cost subCost = CostEsitimaterFactory.estimate((QueryTreeNode) sub);
            rowCount += subCost.getRowCount();
            scanRowCount += subCost.getScanCount();
            // 如果两个不在一个节点上，就会产生网络开销，需要把sub的数据传送到merge节点上
            if (merge.getDataNode() != null) {
                if (!merge.getDataNode().equals(sub.getDataNode())) {
                    networkCost += subCost.getRowCount();
                }
            }
        }

        if (query.getLimitFrom() != null
            && (query.getLimitFrom() instanceof Long || query.getLimitFrom() instanceof Long)
            && (Long) query.getLimitFrom() != 0 && query.getLimitTo() != null
            && (query.getLimitTo() instanceof Long || query.getLimitTo() instanceof Long)
            && (Long) query.getLimitTo() != 0) {
            rowCount = ((Long) query.getLimitTo() - (Long) query.getLimitFrom());
        }

        cost.setRowCount(rowCount);
        cost.setDiskIO(io);
        cost.setNetworkCost(networkCost);
        cost.setIsOnFly(true);
        cost.setScanCount(scanRowCount);
        return cost;
    }

}
