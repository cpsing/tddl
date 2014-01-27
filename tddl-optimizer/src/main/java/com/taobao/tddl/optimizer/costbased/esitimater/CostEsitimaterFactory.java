package com.taobao.tddl.optimizer.costbased.esitimater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;

/**
 * 计算结构树的cost
 * 
 * @since 5.0.0
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
     * 根据索引和查询的filter条件，估算记录数
     * 
     * @param tableRowCount 表记录数
     * @param filters 查询条件
     * @param index 选择的索引
     * @param indexStat 索引的统计信息，如果为null则按照经验值预算
     * @return
     */
    public static long estimateRowCount(long tableRowCount, List<IFilter> filters, IndexMeta index,
                                        KVIndexStat indexStat) {
        // indexMeta
        if (filters == null || filters.isEmpty()) {
            return tableRowCount;
        }

        Map<String, Double> columnAndColumnCountItSelectivity = new HashMap();
        if (index != null && indexStat != null) {
            Double columnCountEveryKeyColumnSelect = ((double) index.getKeyColumns().size())
                                                     * (1.0 / indexStat.getDistinctKeys());
            for (ColumnMeta cm : index.getKeyColumns()) {
                columnAndColumnCountItSelectivity.put(cm.getName(), columnCountEveryKeyColumnSelect);
            }
        }

        long count = tableRowCount;
        // 每出现一个运算符，都把现在的行数乘上一个系数
        for (IFilter f : filters) {
            if (f == null) {
                break;
            }

            IBooleanFilter filter = (IBooleanFilter) f;
            Double selectivity = null;
            if (filter.getColumn() instanceof IColumn) {
                String columnName = ((IColumn) filter.getColumn()).getColumnName();
                if (columnAndColumnCountItSelectivity.containsKey(columnName)) {
                    selectivity = columnAndColumnCountItSelectivity.get(columnName);
                }
            }

            if (selectivity == null) {
                selectivity = CostEsitimaterFactory.selectivity(filter.getOperation());
            }

            count *= selectivity;
        }
        return count;
    }

    /**
     * 参考derby数据库实现
     */
    private static double selectivity(OPERATION operator) {
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
