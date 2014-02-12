package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.KVIndexStat;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 索引选择
 * 
 * <pre>
 * 索引选择策略：
 * 1. 根据选择条件查询，计算出开销最小
 * 2. 根据选择的列，找出全覆盖的索引 (顺序和查询顺序一致，前缀查询)
 * </pre>
 * 
 * @author Dreamond
 */
public class IndexChooser {

    private static final int initialScore = 10000;

    public static IndexMeta findBestIndex(TableMeta tableMeta, List<ISelectable> columns, List<IFilter> filters,
                                          String tablename, Map<String, Object> extraCmd) {
        if (!chooseIndex(extraCmd)) {
            return null;
        }

        List<IndexMeta> indexs = tableMeta.getIndexs();
        if (indexs.isEmpty()) {
            return null;
        }

        Map<Object, List<IFilter>> columnFilters = FilterUtils.toColumnFiltersMap(filters);
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

            KVIndexStat kvIndexStat = OptimizerContext.getContext()
                .getStatManager()
                .getKVIndex(indexs.get(i).getName());
            List<ISelectable> indexColumns = OptimizerUtils.columnMetaListToIColumnList(indexs.get(i).getKeyColumns(),
                tablename);
            for (int j = 0; j < indexColumns.size(); j++) {
                boolean isContain = false;
                if (columnFilters.isEmpty()) {// 此时以columns为准
                    isContain = columns.contains(indexColumns.get(j));
                } else {
                    isContain = columnFilters.containsKey(indexColumns.get(j));
                }

                if (isContain) {
                    scores[i] = (int) CostEsitimaterFactory.estimateRowCount(scores[i],
                        columnFilters.get((indexColumns.get(j))),
                        indexs.get(i),
                        kvIndexStat);
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
     * 根据查询字段，查找一个索引包含所有选择列，并且包含的无关列最少则选择该索引
     */
    public static IndexMeta findBestIndexByAllColumnsSelected(TableMeta tableMeta, List<ISelectable> queryColumns,
                                                              Map<String, Object> extraCmd) {
        if (!chooseIndex(extraCmd)) {
            return null;
        }

        IndexMeta indexChoosed = null;
        int theLeastColumnsNumber = Integer.MAX_VALUE;
        List<IndexMeta> indexs = tableMeta.getIndexs();
        for (int i = 0; i < indexs.size(); i++) {
            List<ISelectable> indexColumns = OptimizerUtils.columnMetaListToIColumnList(indexs.get(i).getKeyColumns());
            if (indexColumns.containsAll(queryColumns)) {
                if (theLeastColumnsNumber > indexs.get(i).getKeyColumns().size()) {
                    theLeastColumnsNumber = indexs.get(i).getKeyColumns().size();
                    indexChoosed = indexs.get(i);
                }
            }
        }

        return indexChoosed;
    }

    private static boolean chooseIndex(Map<String, Object> extraCmd) {
        String ifChooseIndex = ObjectUtils.toString(GeneralUtil.getExtraCmdString(extraCmd,
            ExtraCmd.CHOOSE_INDEX));
        // 默认返回true
        if (StringUtils.isEmpty(ifChooseIndex)) {
            return true;
        } else {
            return BooleanUtils.toBoolean(ifChooseIndex);
        }
    }

}
