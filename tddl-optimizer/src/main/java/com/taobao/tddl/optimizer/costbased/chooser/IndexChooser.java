package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
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
        if (BooleanUtils.toBoolean(ifChooseIndex) == false) {
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

            List<ISelectable> indexColumns = OptimizerUtils.columnMetaListToIColumnList(indexs.get(i).getKeyColumns(),
                tablename);

            for (int j = 0; j < indexColumns.size(); j++) {
                if (columns.contains(indexColumns.get(j))) {
                    scores[i] = (int) estimateRowCount(scores[i],
                        columnFilters.get(((IColumn) indexColumns.get(j))),
                        indexs.get(i));
                    scores[i] -= 1; // 命中一个主键字段
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

    /**
     * 如果某个索引包含所有选择列，并且包含的无关列最少则选择该索引
     */
    public static IndexMeta findIndexWithAllColumnsSelected(List<IndexMeta> indexs, TableNode qn) {
        IndexMeta indexChoosed = null;
        int theIndexOfTheIndexWithLeastColumns = -1;
        int theLeastColumnsNumber = Integer.MAX_VALUE;
        List<ISelectable> queryColumns = qn.getColumnsRefered();
        for (int i = 0; i < indexs.size(); i++) {
            List<ISelectable> indexColumns = OptimizerUtils.columnMetaListToIColumnList(indexs.get(i).getKeyColumns(),
                indexs.get(i).getTableName());

            if (indexColumns.containsAll(queryColumns)) {
                if (theLeastColumnsNumber > indexs.get(i).getKeyColumns().size()) {
                    theLeastColumnsNumber = indexs.get(i).getKeyColumns().size();
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
        if (filters == null || filters.isEmpty()) {
            return oldCount;
        }

        Map<String, Double> columnAndColumnCountItSelectivity = new HashMap();
        KVIndexStat indexStat = null;
        if (index != null) {
            // TODO indexStat =
            // oc.getStatisticsManager().getStatistics(index.getName());
            indexStat = new KVIndexStat(null, 0);

            if (indexStat != null) {
                // 选择度越小，代表索引查找的代价更小，比如选择读为1时，即为精确查找，如果选择度趋向无穷大，即为全表扫描
                Double columnCountEveryKeyColumnSelect = ((double) index.getKeyColumns().size())
                                                         * (1 / indexStat.getDistinct_keys());
                for (ColumnMeta cm : index.getKeyColumns()) {
                    columnAndColumnCountItSelectivity.put(cm.getName(), columnCountEveryKeyColumnSelect);
                }
            }
        }

        long count = oldCount;
        // 每出现一个运算符，都把现在的行数乘上一个系数
        for (IFilter filter : filters) {
            Double selectivity = null;
            IColumn column = null;
            if (((IBooleanFilter) filter).getColumn() instanceof IColumn) {
                column = (IColumn) ((IBooleanFilter) filter).getColumn();
            } else if (((IBooleanFilter) filter).getValue() instanceof IColumn) {
                column = (IColumn) ((IBooleanFilter) filter).getValue();
            }

            if (column != null) {
                selectivity = columnAndColumnCountItSelectivity.get(column.getColumnName());
                if (selectivity == null) {
                    selectivity = CostEsitimaterFactory.selectivity(filter.getOperation()); // 使用默认值
                }

                count *= selectivity;
            }
        }

        return count;
    }
}
