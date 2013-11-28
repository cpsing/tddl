package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.DataNodeChooser;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.rule.model.Field;
import com.taobao.tddl.rule.model.TargetDB;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * <pre>
 * 1. 根据Rule计算分库分表，并设置执行计划的executeOn()
 * 2. 如果存在多个执行目标库，构造为Merge查询树
 * 
 * </pre>
 * 
 * @since 5.1.0
 */
public class DataNodeChooserImpl implements DataNodeChooser {

    private static final Logger logger = LoggerFactory.getLogger(DataNodeChooserImpl.class);

    public ASTNode shard(ASTNode dne, Map<Integer, ParameterContext> parameterSettings, Map<String, Comparable> extraCmd)
                                                                                                                         throws QueryException {
        if (dne instanceof DMLNode) {
            if (dne instanceof InsertNode) {
                return this.shardInsert((InsertNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof UpdateNode) {
                return this.shardUpdate((UpdateNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof DeleteNode) {
                return this.shardDelete((DeleteNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof PutNode) {
                return this.shardPut((PutNode) dne, parameterSettings, extraCmd);
            }
        }
        if (dne instanceof QueryTreeNode) {
            return this.shardQuery((QueryTreeNode) dne, parameterSettings, extraCmd);
        }

        return dne;
    }

    private QueryTreeNode shardQuery(QueryTreeNode dne, Map<Integer, ParameterContext> parameterSettings,
                                     Map<String, Comparable> extraCmd) throws QueryException {
        if (dne instanceof QueryNode) {
            QueryNode q = (QueryNode) dne;
            if (q.getChild() != null) {
                QueryTreeNode child = q.getChild();
                child = this.shardQuery(child, parameterSettings, extraCmd);
                q.setChild(child);
                q.executeOn(child.getDataNode());
                return q;
            }
        }

        if (dne instanceof TableNode) {
            KVIndexNode q = (KVIndexNode) dne;
            boolean isIn = false;
            Object inColumn = null;
            IFilter f = FilterUtils.and(q.getKeyFilter(), q.getResultFilter());
            f = FilterUtils.and(f, q.getOtherJoinOnFilter());
            // TODO shenxun : 这里的最后一个isWrite的属性，目前还未实现，所以，在切换时要全部停摆。
            List<TargetDB> dataNodeChoosed = null;
            boolean isTraceSource = true;
            // 只有当需要全表扫描时，才可能出现or关系
            if (f != null && f instanceof ILogicalFilter) {
                if (f.getOperation().equals(OPERATION.OR)) {
                    f = null;
                    isTraceSource = false;
                }
            }

            dataNodeChoosed = shard(q.getTableName(), q.getIndexName(), f, isTraceSource, true);
            if (dataNodeChoosed == null || dataNodeChoosed.isEmpty()) {
                throw new EmptyResultFilterException("空记录");
            }

            QueryTreeNode queryAfterChooseDataNode = buildMerge(q, dataNodeChoosed, isIn, inColumn);
            return queryAfterChooseDataNode;

        }

        return null;
    }

    private ASTNode shardInsert(InsertNode dne, Map<Integer, ParameterContext> parameterSettings,
                                Map<String, Comparable> extraCmd) {

        String indexName = null;
        if (dne.getNode() instanceof KVIndexNode) {
            indexName = ((KVIndexNode) dne.getNode()).getIndexName();
        } else {
            indexName = dne.getNode().getTableMeta().getPrimaryIndex().getName();
        }

        IFilter insertFilter = null;
        if (dne.getColumns().size() == 1) {
            IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
            f.setOperation(OPERATION.EQ);
            f.setColumn(dne.getColumns().get(0));
            f.setValue(dne.getValues().get(0));
            insertFilter = f;
        } else {
            ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter();
            and.setOperation(OPERATION.AND);
            for (int i = 0; i < dne.getColumns().size(); i++) {
                Comparable c = dne.getColumns().get(i);
                IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                f.setOperation(OPERATION.EQ);
                f.setColumn(c);
                f.setValue(dne.getValues().get(i));
                and.addSubFilter(f);
            }

            insertFilter = and;
        }

        // 根据规则计算
        List<TargetDB> dataNodeChoosed = shard(dne.getTableMeta().getTableName(), indexName, insertFilter, false, true);
        TargetDB itarget = dataNodeChoosed.get(0);
        dne.executeOn(itarget.getDbIndex());

        if (itarget.getTableNameMap() != null && itarget.getTableNameMap().size() == 1) {
            KVIndexNode query = new KVIndexNode(indexName);
            // 设置为物理的表
            query.executeOn(itarget.getDbIndex());
            query.setActualTableName(itarget.getTableNameMap().keySet().iterator().next());
            dne = query.insert(dne.getColumns(), dne.getValues());
            dne.executeOn(query.getDataNode());
            dne.build();
            return dne;
        } else {
            throw new IllegalArgumentException("insert not support muti tables");
        }
    }

    private ASTNode shardUpdate(UpdateNode dne, Map<Integer, ParameterContext> parameterSettings,
                                Map<String, Comparable> extraCmd) throws QueryException {
        QueryTreeNode qtn = this.shardQuery(dne.getNode(), parameterSettings, extraCmd);
        List<ASTNode> subs = new ArrayList();
        if (qtn instanceof MergeNode) {
            subs.addAll(qtn.getChildren());
        } else {
            subs.add(qtn);
        }

        if (subs.size() == 1) {
            return this.buildOneQueryUpdate((QueryTreeNode) subs.get(0), dne.getColumns(), dne.getValues());
        }

        MergeNode updateMerge = new MergeNode();
        for (ASTNode sub : subs) {
            updateMerge.merge(this.buildOneQueryUpdate((QueryTreeNode) sub, dne.getColumns(), dne.getValues()));
        }
        updateMerge.executeOn(updateMerge.getChild().getDataNode());
        return updateMerge;
    }

    private ASTNode shardDelete(DeleteNode dne, Map<Integer, ParameterContext> parameterSettings,
                                Map<String, Comparable> extraCmd) throws QueryException {

        QueryTreeNode qtn = this.shardQuery(dne.getNode(), parameterSettings, extraCmd);
        List<ASTNode> subs = new ArrayList();
        if (qtn instanceof MergeNode) {
            subs.addAll(qtn.getChildren());
        } else {
            subs.add(qtn);
        }

        if (subs.size() == 1) {
            return this.buildOneQueryDelete((QueryTreeNode) subs.get(0));
        }

        MergeNode deleteMerge = new MergeNode();
        for (ASTNode sub : subs) {
            deleteMerge.merge(this.buildOneQueryDelete((QueryTreeNode) sub));
        }
        deleteMerge.executeOn(deleteMerge.getChild().getDataNode());
        return deleteMerge;
    }

    private ASTNode shardPut(PutNode dne, Map<Integer, ParameterContext> parameterSettings,
                             Map<String, Comparable> extraCmd) {
        String indexName = null;
        if (dne.getNode() instanceof KVIndexNode) {
            indexName = ((KVIndexNode) dne.getNode()).getIndexName();
        } else {
            indexName = dne.getNode().getTableMeta().getPrimaryIndex().getName();
        }

        IFilter insertFilter = null;
        if (dne.getColumns().size() == 1) {
            IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
            f.setOperation(OPERATION.EQ);
            f.setColumn(dne.getColumns().get(0));
            f.setValue(dne.getValues().get(0));
            insertFilter = f;

        } else {
            ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter();
            and.setOperation(OPERATION.AND);
            for (int i = 0; i < dne.getColumns().size(); i++) {
                Comparable c = dne.getColumns().get(i);
                IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                f.setOperation(OPERATION.EQ);
                f.setColumn(c);
                f.setValue(dne.getValues().get(i));
                and.addSubFilter(f);
            }

            insertFilter = and;
        }

        List<TargetDB> dataNodeChoosed = shard(dne.getTableMeta().getTableName(), indexName, insertFilter, false, true);
        TargetDB itarget = dataNodeChoosed.get(0);
        dne.executeOn(itarget.getDbIndex());

        if (itarget.getTableNameMap() != null && itarget.getTableNameMap().size() == 1) {
            KVIndexNode query = new KVIndexNode(indexName);
            query.executeOn(itarget.getDbIndex());
            query.setActualTableName(itarget.getTableNameMap().keySet().iterator().next());
            dne = query.put(dne.getColumns(), dne.getValues());
            dne.executeOn(query.getDataNode());
            dne.build();
            return dne;
        } else {
            throw new IllegalArgumentException("insert not support muti tables");
        }
    }

    public ASTNode shard(KVIndexNode dne, IFilter filter, Map<Integer, ParameterContext> parameterSettings,
                         Map<String, Comparable> extraCmd) throws QueryException {
        KVIndexNode q = (KVIndexNode) dne;
        boolean isIn = false;
        Object inColumn = null;

        // TODO shenxun : 这里的最后一个isWrite的属性，目前还未实现，所以，在切换时要全部停摆。
        List<TargetDB> dataNodeChoosed = null;
        boolean isTraceSource = true;
        // 只有当需要全表扫描时，才可能出现or关系
        if (filter != null && filter instanceof ILogicalFilter) {
            if (filter.getOperation().equals(OPERATION.OR)) {
                filter = null;
                isTraceSource = false;
            }
        }

        dataNodeChoosed = shard(q.getTableName(), q.getIndexName(), filter, isTraceSource, true);
        if (dataNodeChoosed == null || dataNodeChoosed.isEmpty()) {
            throw new EmptyResultFilterException("");
        }

        return buildMerge(q, dataNodeChoosed, isIn, inColumn);
    }

    // =============== helper method =================

    private List<TargetDB> shard(String logicalTableName, String logicalIndexName, IFilter f, boolean isTraceSource,
                                 boolean isWrite) {
        if (logger.isDebugEnabled()) {
            logger.warn("shard debug:\n");
            logger.warn("indexName:" + logicalIndexName);
            logger.warn("filter:" + f);
            logger.warn("isTraceSource:" + isTraceSource);
            logger.warn("isWrite:" + isWrite);
        }

        // String tableName = logicalIndexName.split("\\.")[0];
        TableMeta ts = OptimizerContext.getContext().getSchemaManager().getTable(logicalTableName);
        if (ts == null) {
            throw new RuntimeException("can't find logic table " + logicalTableName);
        }

        return OptimizerContext.getContext().getRule().shard(logicalIndexName, f, isWrite);
    }

    private static class OneDbNodeWithCount {

        List<QueryTreeNode> subs          = new ArrayList();
        long                totalRowCount = 0;
    }

    private QueryTreeNode buildMerge(TableNode q, List<TargetDB> dataNodeChoosed, boolean isIn, Object column)
                                                                                                              throws QueryException {

        long maxRowCount = 0;
        String maxRowCountDataNode = dataNodeChoosed.get(0).getDbIndex();
        List<List<QueryTreeNode>> subs = new ArrayList();
        boolean needCopy = true;

        // 单库单表是大多数场景，此时无需复制执行计划
        if (dataNodeChoosed != null && dataNodeChoosed.size() == 1
            && dataNodeChoosed.get(0).getTableNameMap().size() == 1) {
            needCopy = false;
        }

        for (TargetDB target : dataNodeChoosed) {
            OneDbNodeWithCount oneDbNodeWithCount = buildMergeInOneDB(target.getDbIndex(),
                q,
                target.getTableNameMap(),
                isIn,
                column,
                needCopy);

            if (!oneDbNodeWithCount.subs.isEmpty()) {
                subs.add(oneDbNodeWithCount.subs);
            }

            if (oneDbNodeWithCount.totalRowCount > maxRowCount) {
                maxRowCount = oneDbNodeWithCount.totalRowCount;
                maxRowCountDataNode = target.getDbIndex();
            }

        }

        if (subs.isEmpty()) {
            throw new EmptyResultFilterException("空结果");
        }

        if (subs.size() == 1 && subs.get(0).size() == 1) {
            return subs.get(0).get(0);
        }

        MergeNode merge = new MergeNode();
        List<IOrderBy> mergeOrderBys = new ArrayList();
        for (IOrderBy order : q.getOrderBys()) {
            IOrderBy newOrder = order.copy();
            if (q.getAlias() != null) {
                newOrder.setTableName(q.getAlias());
            }

            if (order.getAlias() != null) {
                newOrder.setColumnName(order.getAlias());
                newOrder.getColumn().setAlias(null);
            }
            mergeOrderBys.add(newOrder);
        }

        merge.setOrderBys(mergeOrderBys);
        merge.executeOn("localhost");// 随意的一个名字
        merge.setAlias(q.getAlias());
        merge.setSubQuery(q.isSubQuery());
        merge.executeOn(maxRowCountDataNode);
        for (List<QueryTreeNode> subList : subs) {
            for (QueryTreeNode sub : subList) {
                merge.merge(sub);
            }
        }

        merge.build();
        return merge;
    }

    private OneDbNodeWithCount buildMergeInOneDB(String executeOn, TableNode q, Map<String, Field> tabMap,
                                                 boolean isIn, Object column, boolean needCopy) throws QueryException {

        long totalRowCount = 0;
        OneDbNodeWithCount oneDbNodeWithCount = new OneDbNodeWithCount();

        for (String targetTable : tabMap.keySet()) {
            TableNode sub = null;
            if (needCopy) {
                sub = q.copy();
            } else {
                sub = q;
            }

            // TODO tddl的traceSource在分库不分表，和全表扫描时无法使用
            // 等待TDDL修改 mengshi
            if (tabMap.get(targetTable) != null && tabMap.get(targetTable).getSourceKeys() != null) {
                // 这里仅仅对In做traceSource
                if (sub.getKeyFilter() != null) {
                    IFilter keyFilterAfterTraceSource = (IFilter) sub.getKeyFilter().copy();
                    traceSourceInFilter(keyFilterAfterTraceSource, tabMap.get(targetTable).getSourceKeys());
                    sub.setKeyFilter(keyFilterAfterTraceSource);
                }

                if (sub.getResultFilter() != null) {
                    IFilter valueFilterAfterTraceSource = (IFilter) sub.getResultFilter().copy();
                    traceSourceInFilter(valueFilterAfterTraceSource, tabMap.get(targetTable).getSourceKeys());
                    sub.setResultFilter(valueFilterAfterTraceSource);
                }
            }

            sub.select(sub.getColumnsSelected());
            // sub.setDbName(targetTable);
            sub.executeOn(executeOn);

            try {
                Cost childCost = null;
                childCost = CostEsitimaterFactory.estimate(sub);
                totalRowCount += childCost.getRowCount();
            } catch (StatisticsUnavailableException e) {
                throw new QueryException(e);
            }
            oneDbNodeWithCount.subs.add(sub);
        }

        oneDbNodeWithCount.totalRowCount = totalRowCount;
        return oneDbNodeWithCount;
    }

    private void traceSourceInFilter(IFilter filter, Map<String, Set<Object>> sourceKeys) {
        if (sourceKeys == null || filter == null) {
            return;
        }

        if (filter instanceof IBooleanFilter) {
            if (filter.getOperation().equals(OPERATION.IN)) {
                ISelectable s = (ISelectable) ((IBooleanFilter) filter).getColumn();
                List<Comparable> values = null;
                if (sourceKeys.get(s.getColumnName()) == null) {
                    values = new ArrayList();
                } else {
                    values = new ArrayList(sourceKeys.get(s.getColumnName()));
                }

                if (!values.isEmpty()) {
                    ((IBooleanFilter) filter).setValues(values);
                }
            }
        } else if (filter instanceof ILogicalFilter) {
            if (!filter.getOperation().equals(OPERATION.AND)) {
                throw new RuntimeException("This is impossible");
            }

            for (IFilter subFilter : ((ILogicalFilter) filter).getSubFilter()) {
                if (subFilter.getOperation().equals(OPERATION.IN)) {
                    ISelectable s = (ISelectable) ((IBooleanFilter) subFilter).getColumn();
                    Set<Object> sourceKey = sourceKeys.get(s.getColumnName());
                    if (sourceKey != null) {// 走了规则,在in中有sourceTrace
                        List<Comparable> values = new ArrayList(sourceKey);
                        if (!values.isEmpty()) {
                            ((IBooleanFilter) subFilter).setValues(values);
                        }
                    }
                }
            }
        }
    }

    private UpdateNode buildOneQueryUpdate(QueryTreeNode sub, List columns, List values) {
        if (sub instanceof TableNode) {
            UpdateNode update = ((TableNode) sub).update(columns, values);
            update.executeOn(sub.getDataNode());
            return update;
        } else {
            throw new UnsupportedOperationException("update中暂不支持按照索引进行查询");
        }
    }

    private DeleteNode buildOneQueryDelete(QueryTreeNode sub) {
        if (sub instanceof TableNode) {
            DeleteNode delete = ((TableNode) sub).delete();
            delete.executeOn(sub.getDataNode());
            return delete;
        } else {
            throw new UnsupportedOperationException("delete中暂不支持按照索引进行查询");
        }
    }

}
