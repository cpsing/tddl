package com.taobao.tddl.optimizer.costbased.esitimater;

import java.util.List;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableStat;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author Dreamond
 */
public class QueryNodeCostEstimater implements QueryTreeCostEstimater {

    public Cost estimate(QueryTreeNode q) throws StatisticsUnavailableException {
        QueryTreeNode query = (QueryTreeNode) q;
        Cost cost = new Cost();
        long rowCount = 0;
        long initRowCount = 0;
        long scanRowCount = 0;

        boolean isOnfly = false;
        // 索引
        IndexMeta index = null;
        // 索引的选择度
        KVIndexStat indexStat = null;
        // step1.估算行数
        if (query instanceof QueryNode) {
            // 查询对象是另一个查询，说明数据是on fly的，根据子查询提供的行数来确定初始行数
            Cost childCost = CostEsitimaterFactory.estimate(((QueryNode) query).getChild());
            initRowCount = childCost.getRowCount();
            isOnfly = true;
        } else if (query instanceof TableNode) {
            // 查询对象是一个物理表，则根据表的统计信息来获取初始行数
            isOnfly = false;
            index = ((TableNode) query).getIndexUsed();
            TableStat stat = OptimizerContext.getContext()
                .getStatManager()
                .getTable(((TableNode) query).getTableMeta().getTableName());
            indexStat = OptimizerContext.getContext().getStatManager().getKVIndex(index.getName());
            if (stat != null) {
                initRowCount = stat.getTableRows();
            } else {
                initRowCount = 1000;
            }
        }

        List<IFilter> keyFilters = FilterUtils.toDNFNode(query.getKeyFilter());
        List<IFilter> valueFilters = FilterUtils.toDNFNode(query.getResultFilter());

        // 主键是唯一的，如果在主键上进行了=操作，最后结果肯定不超过1
        // 对于唯一的列也是同理，但是现在还不支持
        // TODO:暂时没有考虑倒排索引
        if (this.isAllEqualOrIS(keyFilters) && index != null && index.isPrimaryKeyIndex()) {
            rowCount = 1;
            scanRowCount = 1;
        } else if (query.getLimitFrom() != null
                   && (query.getLimitFrom() instanceof Long || query.getLimitFrom() instanceof Long)
                   && (Long) query.getLimitFrom() != 0 && query.getLimitTo() != null
                   && (query.getLimitTo() instanceof Long || query.getLimitTo() instanceof Long)
                   && (Long) query.getLimitTo() != 0) {
            // 对于包含limit的查询，使用limit提供的结果
            rowCount = (Long) query.getLimitTo() - (Long) query.getLimitFrom();
            scanRowCount = CostEsitimaterFactory.estimateRowCount(initRowCount, keyFilters, index, indexStat);
        } else if (query.getLimitFrom() != null || query.getLimitTo() != null) {
            rowCount = CostEsitimaterFactory.estimateRowCount(initRowCount, keyFilters, index, indexStat) / 2;
            scanRowCount = rowCount;
            rowCount = CostEsitimaterFactory.estimateRowCount(rowCount, valueFilters, index, indexStat);
        } else {
            // 对于其他情况，则根据约束条件进行推算
            rowCount = CostEsitimaterFactory.estimateRowCount(initRowCount, keyFilters, index, indexStat);
            scanRowCount = rowCount;
            rowCount = CostEsitimaterFactory.estimateRowCount(rowCount, valueFilters, index, indexStat);
        }

        long networkCost = 0;
        // step2.估计网络开销
        if (isOnfly) {
            if (query.getDataNode() == null
                || (query.getDataNode().equals(((QueryNode) query).getChild().getDataNode()))) {
                // 如果当前的查询和子查询在一台机器上执行，则网络开销为0
                networkCost = 0;
            } else {
                // 如果当前的查询和子查询不在一台机器上，则需要将子查询的数据传输到当前查询的机器上
                // 所以网络开销就为子查询结果的行数
                // （目前只用行数作为开销的依据，没有考虑字段的大小等复杂因素）
                networkCost = initRowCount;
            }
        } else {
            // 如果是对物理表进行查询，则不需要经过网络传输，网络开销为0
            networkCost = 0;
        }

        cost.setRowCount(rowCount);
        cost.setNetworkCost(networkCost);
        cost.setScanCount(scanRowCount);
        return cost;
    }

    private boolean isAllEqualOrIS(List<IFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            return false;
        }

        for (IFilter filter : filters) {
            if (filter.getOperation() == OPERATION.IS && filter.getOperation() != OPERATION.EQ) {
                return false;
            }
        }
        return true;
    }

}
