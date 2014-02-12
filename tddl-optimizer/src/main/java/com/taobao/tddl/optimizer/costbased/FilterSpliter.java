package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.chooser.IndexChooser;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 条件分离器 简单来说 一个查询 A = 1 and B = 2 <br/>
 * 需要分析出哪些是key filter?哪些是 post filter( ResultFilter) <br/>
 * 在这里进行分离 key filter就是能走索引的那些filter，post filter就是必须遍历结果集的那些filter
 * 
 * @author Dreamond
 * @since 5.0.0
 */
public class FilterSpliter {

    /**
     * 根据where中的所有条件按照or进行分隔，生成多个query请求. (主要考虑部分存储引擎不支持or语法)
     */
    public static List<QueryTreeNode> splitByDNF(TableNode node, Map<String, Object> extraCmd) throws QueryException {
        if (node.getWhereFilter() == null) {
            return new LinkedList<QueryTreeNode>();
        }

        if (!FilterUtils.isDNF(node.getWhereFilter())) {
            throw new IllegalArgumentException("not dnf!!! fuck!!\n" + node.getWhereFilter());
        }

        List<QueryTreeNode> subQueries = new LinkedList<QueryTreeNode>();
        List<List<IFilter>> DNFNodes = FilterUtils.toDNFNodesArray(node.getWhereFilter());
        // 判断是否要处理OR转化为index merge，比如mysql可以push
        // 比如hbase/bdb这一类，OR条件目前只能是主键索引，由执行器层面去解决
        // 如果涉及多字段时就需要拆分为index merge，同时做uniq
        boolean needCopy = (DNFNodes.size() > 1);
        if (isOptimizeIndexMerge(extraCmd) || DNFNodes.size() == 1) {
            for (List<IFilter> DNFNode : DNFNodes) {
                List columns = Arrays.asList(FilterUtils.toColumnFiltersMap(DNFNode).keySet().toArray());
                String tablename = node.getTableName();
                IndexMeta index = IndexChooser.findBestIndex(node.getTableMeta(), columns, DNFNode, tablename, extraCmd);
                if (index == null) {
                    index = node.getTableMeta().getPrimaryIndex();
                }
                // 如果找不到对应条件的主键，可能需要全表扫描
                TableNode subQuery = node;
                if (needCopy) {
                    subQuery = node.deepCopy();
                }

                subQuery.useIndex(index);
                if (index == null) {// 如果无主键进行全表扫描
                    subQuery.setFullTableScan(true);
                }

                Map<FilterType, IFilter> filters = splitByIndex(DNFNode, subQuery);
                subQuery.setKeyFilter(filters.get(FilterType.IndexQueryKeyFilter));
                subQuery.setIndexQueryValueFilter(filters.get(FilterType.IndexQueryValueFilter));
                subQuery.setResultFilter(filters.get(FilterType.ResultFilter));
                subQueries.add(subQuery);
            }
        } else {
            node.useIndex(node.getTableMeta().getPrimaryIndex());// 默认选中主键
            node.setResultFilter(node.getWhereFilter());
        }

        return subQueries;
    }

    /**
     * 将一组filter，根据索引信息拆分为key/indexValue/Value几种分组，keyFilter可以下推到叶子节点减少数据返回
     */
    public static Map<FilterType, IFilter> splitByIndex(List<IFilter> DNFNode, TableNode table) {
        Map<FilterType, IFilter> filters = new HashMap();
        Map<Object, List<IFilter>> columnAndItsFilters = FilterUtils.toColumnFiltersMap(DNFNode);
        IndexMeta index = table.getIndexUsed();
        if (index == null) { // 默认选择主键
            index = table.getTableMeta().getPrimaryIndex();
        }

        List<ISelectable> indexKeyColumns = new ArrayList(0);
        List<ISelectable> indexValueColumns = new ArrayList(0);
        if (index != null) {
            indexKeyColumns = OptimizerUtils.columnMetaListToIColumnList(index.getKeyColumns(), table.getTableName());
            indexValueColumns = OptimizerUtils.columnMetaListToIColumnList(index.getValueColumns(),
                table.getTableName());
        }

        List<IFilter> indexQueryKeyFilters = new LinkedList();
        List<IFilter> indexQueryValueFilters = new LinkedList();
        List<IFilter> resultFilters = new LinkedList(DNFNode);
        for (int i = 0; i < indexKeyColumns.size(); i++) {
            // 不等于,is null, is not null, like 应该按照valueFilter处理
            List<IFilter> fs = columnAndItsFilters.get(indexKeyColumns.get(i));
            if (fs != null) {
                for (IFilter f : fs) {
                    // filter右边不是常量的，不能走索引
                    if (((IBooleanFilter) f).getValue() != null
                        && (((IBooleanFilter) f).getValue() instanceof ISelectable)) {
                        continue;
                    }

                    if (f != null && f.getOperation() != OPERATION.NOT_EQ && f.getOperation() != OPERATION.IS_NOT_NULL
                        && f.getOperation() != OPERATION.IS_NULL && f.getOperation() != OPERATION.LIKE) {
                        indexQueryKeyFilters.add(f);
                    } else {
                        indexQueryValueFilters.add(f);
                    }
                }
                fs.clear();
            }
        }

        for (int i = 0; i < indexValueColumns.size(); i++) {
            // 不等于,is null, is not null, like 应该按照valueFilter处理
            List<IFilter> fs = columnAndItsFilters.get(indexValueColumns.get(i));
            if (fs != null) {
                for (IFilter f : fs) {
                    // filter右边不是常量的，不能走索引
                    if (((IBooleanFilter) f).getValue() != null
                        && (((IBooleanFilter) f).getValue() instanceof ISelectable)) {
                        continue;
                    }

                    indexQueryValueFilters.add(f);
                }
                fs.clear();
            }

        }
        resultFilters.removeAll(indexQueryKeyFilters);
        resultFilters.removeAll(indexQueryValueFilters);

        IFilter indexQueryKeyTree = FilterUtils.DNFToAndLogicTree(indexQueryKeyFilters);
        IFilter indexQueryValueTree = FilterUtils.DNFToAndLogicTree(indexQueryValueFilters);
        IFilter resultTree = FilterUtils.DNFToAndLogicTree(resultFilters);
        filters.put(FilterType.IndexQueryKeyFilter, indexQueryKeyTree);
        filters.put(FilterType.IndexQueryValueFilter, indexQueryValueTree);
        filters.put(FilterType.ResultFilter, resultTree);
        return filters;
    }

    private static boolean isOptimizeIndexMerge(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ExtraCmd.CHOOSE_INDEX_MERGE, false);
    }
}
