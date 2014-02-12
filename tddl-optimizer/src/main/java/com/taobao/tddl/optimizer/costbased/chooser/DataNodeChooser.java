package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;
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
 * @since 5.0.0
 */
public class DataNodeChooser {

    private static final String LOCAL         = "localhost";
    private static Pattern      suffixPattern = Pattern.compile("\\d+$");                      // 提取字符串最后的数字
    private static final Logger logger        = LoggerFactory.getLogger(DataNodeChooser.class);

    public static ASTNode shard(ASTNode dne, Map<Integer, ParameterContext> parameterSettings,
                                Map<String, Object> extraCmd) throws QueryException {
        if (dne instanceof DMLNode) {
            if (dne instanceof InsertNode) {
                return shardInsert((InsertNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof UpdateNode) {
                return shardUpdate((UpdateNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof DeleteNode) {
                return shardDelete((DeleteNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof PutNode) {
                return shardPut((PutNode) dne, parameterSettings, extraCmd);
            }
        }
        if (dne instanceof QueryTreeNode) {
            return shardQuery((QueryTreeNode) dne, parameterSettings, extraCmd);
        }

        return dne;
    }

    private static QueryTreeNode shardQuery(QueryTreeNode qtn, Map<Integer, ParameterContext> parameterSettings,
                                            Map<String, Object> extraCmd) throws QueryException {
        if (qtn instanceof QueryNode) {
            QueryNode query = (QueryNode) qtn;
            QueryTreeNode child = query.getChild();
            child = shardQuery(child, parameterSettings, extraCmd);
            if (child instanceof MergeNode && !child.isExistAggregate()) {
                return buildMergeQuery(query, (MergeNode) child);
            } else {
                query.setChild(child);
                query.setBroadcast(child.isBroadcast());// 传递一下
                query.executeOn(child.getDataNode());
                query.setExistAggregate(child.isExistAggregate());
                return query;
            }
        } else if (qtn instanceof TableNode) {
            // 此时经过join处理后，已经全部转化为kv结构的查询了
            KVIndexNode query = (KVIndexNode) qtn;
            // 构造filter
            IFilter f = FilterUtils.and(query.getKeyFilter(), query.getResultFilter());
            f = FilterUtils.and(f, query.getOtherJoinOnFilter());
            List<TargetDB> dataNodeChoosed = shard(query.getIndexName(), f, true);
            return buildMergeTable(query, dataNodeChoosed);
        } else if (qtn instanceof JoinNode) {
            // Join节点应该在数据量大的一段进行
            boolean isPartitionOnPartition = false;
            JoinNode join = (JoinNode) qtn;
            QueryTreeNode left = join.getLeftNode();
            QueryTreeNode right = join.getRightNode();

            /*
             * 如果是分库键join分库键，并且规则相同，则优化成join merge join
             */
            if (chooseJoinMergeJoinForce(extraCmd)) {
                isPartitionOnPartition = true;// 强制开启
            } else if (chooseJoinMergeJoinByRule(extraCmd)) {
                isPartitionOnPartition = isJoinOnPartition(join).flag; // 根据规则判断
            }

            // 处理子节点
            left = shardQuery(left, parameterSettings, extraCmd);
            right = shardQuery(right, parameterSettings, extraCmd);
            if (isPartitionOnPartition) {
                // 尝试构建join merge join，可能会构建失败
                // 失败原因 :
                // 1. 人肉强制开启join merge join的选项
                // 2. 涉及index kv的join查询结构，index的数据和原始数据的分区方式可能不一致
                QueryTreeNode joinMergeJOin = buildJoinMergeJoin(join, left, right);
                if (joinMergeJOin != null) {
                    return joinMergeJOin;
                }
            }

            // 不做join merge join
            join.setLeftNode(left);
            // NestedLoop情况下
            // 如果右边是多个表，则分库需要再执行层根据左边的结果做
            if (!(right instanceof MergeNode && (join.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP || join.getJoinStrategy() == JoinStrategy.NEST_LOOP_JOIN))) {
                join.setRightNode(right);
            } else {
                if (right.isSubQuery()) {
                    // 子表会采取BLOCK_LOOP_JOIN模式，一次性取完结果
                    join.setRightNode(right);
                } else {
                    right = new MergeNode();
                    // 右边运行时计算
                    ((MergeNode) right).merge(join.getRightNode());
                    ((MergeNode) right).setSharded(false);
                    join.getRightNode().executeOn("undecided");
                    right.setBroadcast(false);
                    right.build();
                    join.setRightNode(right);
                }
            }

            String dataNode = join.getLeftNode().getDataNode();
            // 对于未决的IndexNestedLoop，join应该在左节点执行
            if (right instanceof MergeNode && !((MergeNode) right).isSharded()) {
                join.executeOn(dataNode);
                right.executeOn(join.getDataNode());
            } else {
                // 选择一个执行代价最小的节点
                Cost leftCost = CostEsitimaterFactory.estimate(left);
                Cost rightCost = CostEsitimaterFactory.estimate(right);
                dataNode = leftCost.getRowCount() > rightCost.getRowCount() ? join.getLeftNode().getDataNode() : join.getRightNode()
                    .getDataNode();
                join.executeOn(dataNode);
            }

            join.setBroadcast(left.isBroadcast() && right.isBroadcast());
            return join;
        } else if (qtn instanceof MergeNode) {
            // 一个query出现or可能会走到index merge，会拆分为merge合并两个请求
            // 为merge选择执行节点
            // 很多方案...
            // 两路归并?
            // 都发到一台机器上？
            // 目前的方案是都发到一台机器上
            // 对Merge的每个子节点的rowCount进行排序
            // 找出rowCount最大的子节点
            // merge应该在该机器上执行，其他机器的数据都发送给它
            MergeNode merge = (MergeNode) qtn;
            List<ASTNode> subNodes = merge.getChildren();
            merge = new MergeNode();
            merge.setUnion(((MergeNode) qtn).isUnion());
            long maxRowCount = 0;
            String maxRowCountDataNode = subNodes.get(0).getDataNode();
            for (int i = 0; i < subNodes.size(); i++) {
                QueryTreeNode child = (QueryTreeNode) subNodes.get(i);
                child = shardQuery(child, parameterSettings, extraCmd);
                subNodes.set(i, child);
                Cost childCost = CostEsitimaterFactory.estimate(child);
                if (childCost.getRowCount() > maxRowCount) {
                    maxRowCount = childCost.getRowCount();
                    maxRowCountDataNode = child.getDataNode();
                }
            }

            if (maxRowCountDataNode == null) {
                maxRowCountDataNode = subNodes.get(0).getDataNode();
            }

            merge.executeOn(maxRowCountDataNode);
            merge.merge(subNodes);
            merge.setSharded(true);
            merge.setBroadcast(false);
            merge.build();
            return merge;
        }

        return qtn;
    }

    private static ASTNode shardInsert(InsertNode dne, Map<Integer, ParameterContext> parameterSettings,
                                       Map<String, Object> extraCmd) {

        String indexName = null;
        if (dne.getNode() instanceof KVIndexNode) {
            indexName = ((KVIndexNode) dne.getNode()).getIndexName();
        } else {
            indexName = dne.getNode().getTableMeta().getPrimaryIndex().getName();
        }

        // 根据规则计算
        IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues());
        List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true);
        TargetDB itarget = dataNodeChoosed.get(0);
        dne.executeOn(itarget.getDbIndex());
        if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null && itarget.getTableNameMap().size() == 1) {
            KVIndexNode query = new KVIndexNode(indexName);
            // 设置为物理的表
            query.executeOn(itarget.getDbIndex());
            query.setActualTableName(itarget.getTableNameMap().keySet().iterator().next());
            dne = query.insert(dne.getColumns(), dne.getValues());
            dne.executeOn(query.getDataNode());
            dne.build();
            return dne;
        } else {
            throw new OptimizerException("insert not support muti tables");
        }
    }

    private static ASTNode shardUpdate(UpdateNode dne, Map<Integer, ParameterContext> parameterSettings,
                                       Map<String, Object> extraCmd) throws QueryException {
        QueryTreeNode qtn = shardQuery(dne.getNode(), parameterSettings, extraCmd);
        List<ASTNode> subs = new ArrayList();
        if (qtn instanceof MergeNode) {
            subs.addAll(qtn.getChildren());
        } else {
            subs.add(qtn);
        }

        if (subs.size() == 1) {
            return buildOneQueryUpdate((QueryTreeNode) subs.get(0), dne.getColumns(), dne.getValues());
        }

        MergeNode updateMerge = new MergeNode();
        for (ASTNode sub : subs) {
            updateMerge.merge(buildOneQueryUpdate((QueryTreeNode) sub, dne.getColumns(), dne.getValues()));
        }
        updateMerge.executeOn(updateMerge.getChild().getDataNode());
        return updateMerge;
    }

    private static ASTNode shardDelete(DeleteNode dne, Map<Integer, ParameterContext> parameterSettings,
                                       Map<String, Object> extraCmd) throws QueryException {
        QueryTreeNode qtn = shardQuery(dne.getNode(), parameterSettings, extraCmd);
        List<ASTNode> subs = new ArrayList();
        if (qtn instanceof MergeNode) {
            subs.addAll(qtn.getChildren());
        } else {
            subs.add(qtn);
        }

        if (subs.size() == 1) {
            return buildOneQueryDelete((QueryTreeNode) subs.get(0));
        }

        MergeNode deleteMerge = new MergeNode();
        for (ASTNode sub : subs) {
            deleteMerge.merge(buildOneQueryDelete((QueryTreeNode) sub));
        }
        deleteMerge.executeOn(deleteMerge.getChild().getDataNode());
        return deleteMerge;
    }

    private static ASTNode shardPut(PutNode dne, Map<Integer, ParameterContext> parameterSettings,
                                    Map<String, Object> extraCmd) {
        String indexName = null;
        if (dne.getNode() instanceof KVIndexNode) {
            indexName = ((KVIndexNode) dne.getNode()).getIndexName();
        } else {
            indexName = dne.getNode().getTableMeta().getPrimaryIndex().getName();
        }

        IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues());
        List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true);
        TargetDB itarget = dataNodeChoosed.get(0);
        dne.executeOn(itarget.getDbIndex());
        if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null && itarget.getTableNameMap().size() == 1) {
            KVIndexNode query = new KVIndexNode(indexName);
            query.executeOn(itarget.getDbIndex());
            query.setActualTableName(itarget.getTableNameMap().keySet().iterator().next());
            dne = query.put(dne.getColumns(), dne.getValues());
            dne.executeOn(query.getDataNode());
            dne.build();
            return dne;
        } else {
            throw new OptimizerException("insert not support muti tables");
        }
    }

    // =============== helper method =================

    /**
     * 根据逻辑表名和执行条件计算出执行节点
     */
    private static List<TargetDB> shard(String logicalName, IFilter f, boolean isWrite) {
        boolean isTraceSource = true;
        if (f != null && f instanceof ILogicalFilter) {
            if (f.getOperation().equals(OPERATION.OR)) {
                f = null;
                isTraceSource = false;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.warn("shard debug:\n");
            logger.warn("logicalName:" + logicalName);
            logger.warn("filter:" + f);
            logger.warn("isTraceSource:" + isTraceSource);
            logger.warn("isWrite:" + isWrite);
        }

        String tableName = logicalName.split("\\.")[0];
        OptimizerContext.getContext().getSchemaManager().getTable(tableName); // 验证下表是否存在
        List<TargetDB> dataNodeChoosed = OptimizerContext.getContext().getRule().shard(logicalName, f, isWrite);
        if (dataNodeChoosed == null || dataNodeChoosed.isEmpty()) {
            throw new EmptyResultFilterException("无对应执行节点");
        }

        return dataNodeChoosed;
    }

    private static class OneDbNodeWithCount {

        List<QueryTreeNode> subs          = new ArrayList();
        long                totalRowCount = 0;
    }

    /**
     * 根据执行的目标节点，构建MergeNode
     */
    private static QueryTreeNode buildMergeTable(TableNode table, List<TargetDB> dataNodeChoosed) {
        long maxRowCount = 0;
        String maxRowCountDataNode = dataNodeChoosed.get(0).getDbIndex();
        List<List<QueryTreeNode>> subs = new ArrayList();
        // 单库单表是大多数场景，此时无需复制执行计划
        boolean needCopy = true;
        if (dataNodeChoosed != null && dataNodeChoosed.size() == 1
            && dataNodeChoosed.get(0).getTableNameMap().size() == 1) {
            needCopy = false;
        }

        for (TargetDB target : dataNodeChoosed) {
            OneDbNodeWithCount oneDbNodeWithCount = buildMergeTableInOneDB(table, target, needCopy);
            if (!oneDbNodeWithCount.subs.isEmpty()) {
                subs.add(oneDbNodeWithCount.subs);
            }

            if (oneDbNodeWithCount.totalRowCount > maxRowCount) {
                maxRowCount = oneDbNodeWithCount.totalRowCount;
                maxRowCountDataNode = target.getDbIndex();
            }

        }

        if (subs.isEmpty()) {
            throw new EmptyResultFilterException("无对应执行节点");
        } else if (subs.size() == 1 && subs.get(0).size() == 1) {
            return subs.get(0).get(0); // 只有单库
        } else {
            // 多库执行
            MergeNode merge = new MergeNode();
            merge.executeOn(LOCAL);// 随意的一个名字
            merge.setAlias(table.getAlias());
            merge.setSubQuery(table.isSubQuery());
            merge.setSubAlias(table.getSubAlias());
            merge.executeOn(maxRowCountDataNode);
            for (List<QueryTreeNode> subList : subs) {
                for (QueryTreeNode sub : subList) {
                    merge.merge(sub);
                }
            }
            merge.setBroadcast(false);// merge不可能是广播表
            merge.build();// build过程中会复制子节点的信息
            return merge;
        }
    }

    /**
     * 构建单库的执行节点
     */
    private static OneDbNodeWithCount buildMergeTableInOneDB(TableNode table, TargetDB targetDB, boolean needCopy) {
        long totalRowCount = 0;
        OneDbNodeWithCount oneDbNodeWithCount = new OneDbNodeWithCount();
        Map<String, Field> tabMap = targetDB.getTableNameMap();
        for (String targetTable : tabMap.keySet()) {
            TableNode node = null;
            if (needCopy) {
                node = table.copy();
            } else {
                node = table;
            }

            // tddl的traceSource在分库不分表，和全表扫描时无法使用 mengshi
            if (tabMap.get(targetTable) != null && tabMap.get(targetTable).getSourceKeys() != null) {
                if (node.getKeyFilter() != null) {
                    IFilter keyFilterAfterTraceSource = (IFilter) node.getKeyFilter().copy();
                    traceSourceInFilter(keyFilterAfterTraceSource, tabMap.get(targetTable).getSourceKeys());
                    node.setKeyFilter(keyFilterAfterTraceSource);
                }

                if (node.getResultFilter() != null) {
                    IFilter valueFilterAfterTraceSource = (IFilter) node.getResultFilter().copy();
                    traceSourceInFilter(valueFilterAfterTraceSource, tabMap.get(targetTable).getSourceKeys());
                    node.setResultFilter(valueFilterAfterTraceSource);
                }
            }

            node.setActualTableName(targetTable);
            node.executeOn(targetDB.getDbIndex());
            node.setExtra(getIdentifierExtra((KVIndexNode) node));// 设置标志
            // 暂时先用逻辑表名，以后可能是索引名
            String indexName = null;
            if (node instanceof KVIndexNode) {
                indexName = ((KVIndexNode) node).getKvIndexName();
            } else {
                indexName = node.getTableMeta().getPrimaryIndex().getName();
            }
            node.setBroadcast(OptimizerContext.getContext().getRule().isBroadCast(indexName));

            totalRowCount += CostEsitimaterFactory.estimate(node).getRowCount();
            oneDbNodeWithCount.subs.add(node);
        }

        oneDbNodeWithCount.totalRowCount = totalRowCount;
        return oneDbNodeWithCount;
    }

    /**
     * 更新in操作的values
     */
    private static void traceSourceInFilter(IFilter filter, Map<String, Set<Object>> sourceKeys) {
        if (sourceKeys == null || filter == null) {
            return;
        }

        if (filter instanceof IBooleanFilter) {
            if (filter.getOperation().equals(OPERATION.IN)) {
                ISelectable s = (ISelectable) ((IBooleanFilter) filter).getColumn();
                Set<Object> sourceKey = sourceKeys.get(s.getColumnName());
                if (sourceKey != null && !sourceKey.isEmpty()) {
                    ((IBooleanFilter) filter).setValues(new ArrayList(sourceKey));
                }
            }
        } else if (filter instanceof ILogicalFilter) {
            for (IFilter subFilter : ((ILogicalFilter) filter).getSubFilter()) {
                if (subFilter.getOperation().equals(OPERATION.IN)) {
                    ISelectable s = (ISelectable) ((IBooleanFilter) subFilter).getColumn();
                    Set<Object> sourceKey = sourceKeys.get(s.getColumnName());
                    if (sourceKey != null && !sourceKey.isEmpty()) {// 走了规则,在in中有sourceTrace
                        ((IBooleanFilter) subFilter).setValues(new ArrayList(sourceKey));
                    }
                }
            }
        }
    }

    private static UpdateNode buildOneQueryUpdate(QueryTreeNode sub, List columns, List values) {
        if (sub instanceof TableNode) {
            UpdateNode update = ((TableNode) sub).update(columns, values);
            update.executeOn(sub.getDataNode());
            return update;
        } else {
            throw new NotSupportException("update中暂不支持按照索引进行查询");
        }
    }

    private static DeleteNode buildOneQueryDelete(QueryTreeNode sub) {
        if (sub instanceof TableNode) {
            DeleteNode delete = ((TableNode) sub).delete();
            delete.executeOn(sub.getDataNode());
            return delete;
        } else {
            throw new NotSupportException("delete中暂不支持按照索引进行查询");
        }
    }

    private static QueryTreeNode buildMergeQuery(QueryNode query, MergeNode mergeNode) {
        List<QueryNode> mergeQuerys = new LinkedList<QueryNode>();
        for (ASTNode child : mergeNode.getChildren()) {
            // 在未做shard之前就有存在mergeNode的可能，要识别出来
            // 比如OR条件，可能会被拆分为两个Query的Merge，这个经过build之后，会是Merge套Merge或者是Merge套Query
            if (!(child instanceof MergeNode) && child.getExtra() != null) {
                QueryNode newQuery = query.copy();
                newQuery.setChild((QueryTreeNode) child);
                newQuery.executeOn(child.getDataNode());
                newQuery.setExtra(child.getExtra());
                newQuery.setBroadcast(child.isBroadcast());
                mergeQuerys.add(newQuery);
            }
        }

        if (mergeQuerys.size() > 1) {
            MergeNode merge = new MergeNode();
            merge.setAlias(query.getAlias());
            merge.setSubQuery(query.isSubQuery());
            merge.setSubAlias(query.getSubAlias());
            for (QueryNode q : mergeQuerys) {
                merge.merge(q);
            }

            merge.executeOn(mergeQuerys.get(0).getDataNode()).setExtra(mergeQuerys.get(0).getExtra());
            merge.build();
            return merge;
        } else if (mergeQuerys.size() == 1) {
            return mergeQuerys.get(0);
        } else {
            return query;
        }
    }

    private static boolean chooseJoinMergeJoinByRule(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ExtraCmd.JOIN_MERGE_JOIN_JUDGE_BY_RULE, true);
    }

    private static boolean chooseJoinMergeJoinForce(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ExtraCmd.JOIN_MERGE_JOIN, false);
    }

    private static class PartitionJoinResult {

        List<IBooleanFilter> joinFilters;
        String               joinGroup;
        boolean              broadcast = false;
        boolean              flag      = false; // 成功还是失败
    }

    /**
     * 找到join条件完全是分区键的filter，返回null代表没找到，否则返回join条件
     */
    private static PartitionJoinResult isJoinOnPartition(JoinNode join) {
        QueryTreeNode left = join.getLeftNode();
        QueryTreeNode right = join.getRightNode();

        PartitionJoinResult leftResult = isJoinOnPartitionOneSide(join.getLeftKeys(), left);
        if (!leftResult.flag) {
            return leftResult;
        }

        PartitionJoinResult rightResult = isJoinOnPartitionOneSide(join.getRightKeys(), right);
        if (!rightResult.flag) {
            return rightResult;
        }

        PartitionJoinResult result = new PartitionJoinResult();
        result.broadcast = leftResult.broadcast || rightResult.broadcast;
        result.flag = StringUtils.equalsIgnoreCase(leftResult.joinGroup, rightResult.joinGroup) || result.broadcast;
        result.joinGroup = leftResult.joinGroup;
        result.joinFilters = join.getJoinFilter();
        return result;
    }

    /**
     * 判断一个joinNode的左或则右节点的joinColumns是否和当前节点的分区键完全匹配
     */
    private static PartitionJoinResult isJoinOnPartitionOneSide(List<ISelectable> joinColumns, QueryTreeNode qtn) {
        PartitionJoinResult result = new PartitionJoinResult();
        // 递归拿左边的树
        if (qtn instanceof JoinNode) {
            result = isJoinOnPartition((JoinNode) qtn);
            if (!result.flag) {// 递归失败，直接返回
                result.flag = false;
                return result;
            }

            List<IBooleanFilter> joinOnPatitionFilters = result.joinFilters;
            if (joinOnPatitionFilters == null) {// 底下不满足，直接退出
                result.flag = false;
                return result;
            } else {
                for (ISelectable joinColumn : joinColumns) {
                    ISelectable select = qtn.findColumn(joinColumn);
                    if (!isMatchJoinFilter(joinOnPatitionFilters, select)) {
                        result.flag = false;
                        return result;
                    }
                }

                result.flag = true;
                return result;
            }
        } else if (qtn instanceof QueryNode) {
            // 转一次为query中的select字段
            List<ISelectable> newJoinColumns = new LinkedList<ISelectable>();
            for (ISelectable joinColumn : joinColumns) {
                ISelectable newColumn = qtn.findColumn(joinColumn);
                if (newColumn == null) {
                    return null;
                } else {
                    newJoinColumns.add(newColumn);
                }
            }
            // 获取当前表的分区字段
            return isJoinOnPartitionOneSide(newJoinColumns, (QueryTreeNode) qtn.getChild());
        } else if (qtn instanceof MergeNode) {
            result.flag = false;
            return result; // 直接返回，不处理
        } else {
            if (OptimizerContext.getContext().getRule().isBroadCast(((KVIndexNode) qtn).getKvIndexName())) {
                result.flag = true;
                result.broadcast = true;
                return result;
            }

            // KVIndexNode
            List<String> shardColumns = OptimizerContext.getContext()
                .getRule()
                .getSharedColumns(((KVIndexNode) qtn).getKvIndexName());
            List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
            TableMeta tableMeta = ((KVIndexNode) qtn).getTableMeta();
            for (String shardColumn : shardColumns) {
                columns.add(tableMeta.getColumn(shardColumn));
            }

            String tableName = ((KVIndexNode) qtn).getTableName();
            if (qtn.getAlias() != null) {
                tableName = qtn.getAlias();
            }

            List<ISelectable> partitionColumns = OptimizerUtils.columnMetaListToIColumnList(columns, tableName);

            if (partitionColumns.isEmpty()) {
                result.flag = false;// 没有分库键
                return result;
            }

            // 要求joinColumns必须包含所有的partitionColumns
            for (ISelectable partitionColumn : partitionColumns) {
                boolean isFound = false;
                for (ISelectable joinColumn : joinColumns) {
                    if (joinColumn.getColumnName().equals(partitionColumn.getColumnName())) {// partition无别名
                        isFound = true;
                        break;
                    }
                }

                if (!isFound) {
                    result.flag = false;// 没有分库键
                    return result;
                }
            }

            result.flag = true;
            String indexName = ((KVIndexNode) qtn).getKvIndexName();
            result.joinGroup = OptimizerContext.getContext().getRule().getJoinGroup(indexName);
            return result;
        }
    }

    /**
     * 判断是否是join条件中的一个字段，可能是左表或右边的字段
     */
    private static boolean isMatchJoinFilter(List<IBooleanFilter> joinFilters, ISelectable column) {
        for (IBooleanFilter joinFilter : joinFilters) {
            ISelectable leftJoinColumn = (ISelectable) joinFilter.getColumn();
            ISelectable rightJoinColumn = (ISelectable) joinFilter.getValue();

            if (leftJoinColumn.equals(column) || rightJoinColumn.equals(column)) {
                return true;
            }
        }

        return false;
    }

    private static QueryTreeNode buildJoinMergeJoin(JoinNode join, QueryTreeNode left, QueryTreeNode right) {
        // 根据表名的后缀来判断两个表是不是对应的表
        // 底下的节点已经被优先处理
        // 1. 如果是KvIndexNode，没必要转化为join merge join，直接join即可
        // 2. 如果是QueryNode，底下已经将其转化为merge下套query+child
        // 所以，只要处理MergeNode即可
        Map<String, QueryTreeNode> leftIdentifierExtras = new HashMap<String, QueryTreeNode>();
        List<JoinNode> joins = new ArrayList();
        List<QueryTreeNode> rights = new ArrayList();
        List<QueryTreeNode> lefts = new ArrayList();
        boolean leftBroadCast = false;
        boolean rightBroadCast = false;
        if (left instanceof MergeNode) {
            for (ASTNode child : left.getChildren()) {
                leftIdentifierExtras.put((String) child.getExtra(), (QueryTreeNode) child);
                lefts.add((QueryTreeNode) child);
            }
        } else {
            // 可能是广播表
            leftBroadCast = left.isBroadcast();
        }

        if (right instanceof MergeNode) {
            for (ASTNode child : right.getChildren()) {
                rights.add((QueryTreeNode) child);
            }
        } else {
            // 可能是广播表
            rightBroadCast = right.isBroadcast();
        }

        if (leftBroadCast && rightBroadCast) {
            return join;// 两个广播表之间的join，直接返回
        } else if (leftBroadCast) {// 左边是广播表
            for (QueryTreeNode r : rights) {
                if (r.isExistAggregate()) {// 存在聚合计算，不能展开
                    return null;
                }
                JoinNode newj = join.copy();
                QueryTreeNode newL = left.copy();
                setExecuteOn(newL, r.getDataNode());// 广播表的执行节点跟着右边走
                newj.setLeftNode(newL);
                newj.setRightNode(r);
                newj.executeOn(r.getDataNode());
                newj.setExtra(r.getExtra());
                joins.add(newj);
            }
        } else if (rightBroadCast) {
            for (QueryTreeNode l : lefts) {
                if (l.isExistAggregate()) {// 存在聚合计算，不能展开
                    return null;
                }
                QueryTreeNode newR = right.copy();
                setExecuteOn(newR, l.getDataNode());// 广播表的执行节点跟着右边走
                JoinNode newj = join.copy();
                newj.setLeftNode(l);
                newj.setRightNode(newR);
                newj.executeOn(l.getDataNode());
                newj.setExtra(l.getExtra());
                joins.add(newj);
            }
        } else {
            // 根据后缀，找到匹配的表，生成join
            for (QueryTreeNode r : rights) {
                QueryTreeNode l = leftIdentifierExtras.get(r.getExtra());
                if (l == null) {
                    return null; // 转化失败，直接退回merge join merge的处理
                }

                if (l.isExistAggregate() || r.isExistAggregate()) {// 存在聚合计算，不能展开
                    return null;
                }

                JoinNode newj = join.copy();
                newj.setLeftNode(l);
                newj.setRightNode(r);
                newj.executeOn(l.getDataNode());
                newj.setExtra(l.getExtra()); // 因为left/right的extra相同，只要选择一个即可
                joins.add(newj);
            }
        }

        if (joins.size() > 1) {
            MergeNode merge = new MergeNode();
            for (JoinNode j : joins) {
                merge.merge(j);
            }

            merge.executeOn(joins.get(0).getDataNode()).setExtra(joins.get(0).getExtra());
            merge.build();
            return merge;
        } else if (joins.size() == 1) {
            return joins.get(0);
        }

        return null;
    }

    /**
     * 递归设置executeOn
     */
    private static void setExecuteOn(QueryTreeNode qtn, String dataNode) {
        for (ASTNode node : qtn.getChildren()) {
            setExecuteOn((QueryTreeNode) node, dataNode);
        }

        qtn.executeOn(dataNode);
    }

    /**
     * 根据表名提取唯一标识
     * 
     * <pre>
     * 1. tddl中的分库分表时，比如分16个库，每个库128张表，总共1024张表. 表的顺序为递增，从0000-1023，
     *    此时executeNode就是库名，表名可通过后缀获取，两者结合可以唯一确定一张表
     * 2. cobar中的分库分表，只会分库，不分表，每个库中的表名都一样. 
     *    此时executeNode就是库名，已经可以唯一确定一张表
     * </pre>
     */
    private static String getIdentifierExtra(KVIndexNode child) {
        String tableName = child.getActualTableName();
        if (tableName == null) {
            tableName = child.getIndexName();
        }

        Matcher matcher = suffixPattern.matcher(tableName);
        if (matcher.find()) {
            return (child.getDataNode() + "_" + matcher.group());
        } else {
            return child.getDataNode();
        }
    }

    private static IFilter createFilter(List<ISelectable> columns, List<Object> values) {
        IFilter insertFilter = null;
        if (columns.size() == 1) {
            IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
            f.setOperation(OPERATION.EQ);
            f.setColumn(columns.get(0));
            f.setValue(values.get(0));
            insertFilter = f;
        } else {
            ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter();
            and.setOperation(OPERATION.AND);
            for (int i = 0; i < columns.size(); i++) {
                Comparable c = columns.get(i);
                IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                f.setOperation(OPERATION.EQ);
                f.setColumn(c);
                f.setValue(values.get(i));
                and.addSubFilter(f);
            }

            insertFilter = and;
        }
        return insertFilter;
    }
}
