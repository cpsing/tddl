package com.taobao.tddl.optimizer.costbased;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.TableColumnStat;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * @author Dreamond
 */
public class IndexChooser {

    private static final int initialScore = 10000;

    public static IndexMeta findBestIndex(List<IndexMeta> indexs, List<ISelectable> columns, List<IFilter> filters,
                                          String tablename, Map<String, Comparable> extraCmd) {
        String ifChooseIndex = GeneralUtil.getExtraCmd(extraCmd, ExtraCmd.OptimizerExtraCmd.ChooseIndex);
        if (ifChooseIndex != null && "FALSE".equalsIgnoreCase(ifChooseIndex)) {
            return null;
        }

        if (indexs.isEmpty()) {
            return null;
        }

        Map<Comparable, List<IFilter>> columnFilters = FilterUtils.toColumnFiltersMap(filters);
        int scores[] = new int[indexs.size()];
        for (int i = 0; i < scores.length; i++) {
            scores[i] = initialScore;
        }

        for (int i = 0; i < indexs.size(); i++) {
            // 目前不使用弱一致索引
            if (!indexs.get(i).isStronglyConsistent()) {
                scores[i] = Integer.MAX_VALUE;
                continue;
            }
            List<ISelectable> indexColumns = OptimizerUtils.columnMetaListToIColumnList(Arrays.asList(indexs.get(i)
                .getKeyColumns()), tablename);

            for (int j = 0; j < indexColumns.size(); j++) {
                if (columns.contains(indexColumns.get(j))) {
                    scores[i] = (int) estimateRowCount(scores[i],
                        columnFilters.get(((IColumn) indexColumns.get(j))),
                        indexs.get(i));
                    scores[i] -= 1;
                } else {
                    break;
                }

            }
        }

        for (int i = 0; i < scores.length; i++) {
            scores[i] = initialScore - scores[i];
        }
        int maxIndex = 0;
        int maxScore = scores[maxIndex];
        for (int i = 1; i < scores.length; i++) {
            if (scores[i] > maxScore) {
                maxIndex = i;
                maxScore = scores[i];
            }
        }

        if (maxScore == 0) {
            return null;
        }

        return indexs.get(maxIndex);
    }

    // 如果某个索引包含所有选择列，并且包含的无关列最少则选择该索引
    public static IndexMeta findIndexWithAllColumnsSelected(List<IndexMeta> indexs, TableNode qn) {
        IndexMeta indexChoosed = null;
        int theIndexOfTheIndexWithLeastColumns = -1;
        int theLeastColumnsNumber = Integer.MAX_VALUE;
        List<ISelectable> queryColumns = qn.getColumnsRefered();
        for (int i = 0; i < indexs.size(); i++) {
            List<ISelectable> indexColumns = OptimizerUtils.columnMetaListToIColumnList(Arrays.asList(indexs.get(i)
                .getKeyColumns()), indexs.get(i).getTableName());

            if (indexColumns.containsAll(queryColumns)) {
                if (theLeastColumnsNumber > indexs.get(i).getKeyColumns().length) {
                    theLeastColumnsNumber = indexs.get(i).getKeyColumns().length;
                    theIndexOfTheIndexWithLeastColumns = i;
                }
            }
        }

        if (theIndexOfTheIndexWithLeastColumns != -1) {
            indexChoosed = indexs.get(theIndexOfTheIndexWithLeastColumns);
        }
        return indexChoosed;
    }

    private static long estimateRowCount(long oldCount, List<IFilter> filters, IndexMeta index) {
        // indexMeta
        if (filters == null || filters.isEmpty()) return oldCount;

        Map<String, Double> columnAndColumnCountItSelectivity = new HashMap();
        KVIndexStat indexStat = null;
        TableColumnStat columnStat = null;
        if (index != null) {
            // indexStat =
            // oc.getStatisticsManager().getStatistics(index.getName());

            if (indexStat != null) {
                Double columnCountEveryKeyColumnSelect = ((double) index.getKeyColumns().length)
                                                         * (1 / indexStat.getDistinct_keys());
                for (ColumnMeta cm : index.getKeyColumns()) {
                    columnAndColumnCountItSelectivity.put(cm.getName(), columnCountEveryKeyColumnSelect);
                }
            }
        }

        // OperatorSelectivityTable ost = oc.getOperatorSelectivityTable();
        long count = oldCount;
        // 每出现一个运算符，都把现在的行数乘上一个系数
        for (IFilter filter : filters) {
            if (filter == null) {
                break;
            }
            Double selectivity = null;
            if (((IBooleanFilter) filter).getColumn() instanceof IColumn) {
                String columnName = ((IColumn) ((IBooleanFilter) filter).getColumn()).getColumnName();

                if (columnAndColumnCountItSelectivity.containsKey(columnName)) {
                    selectivity = columnAndColumnCountItSelectivity.get(columnName);
                }
            }

            // if (selectivity == null) {
            // selectivity = ost.getSelectivity(filter.getOperation());
            // }

            count *= selectivity;
        }

        return count;
    }
}
