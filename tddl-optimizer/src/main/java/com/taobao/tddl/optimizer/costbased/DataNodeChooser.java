package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
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
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
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
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinType;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.StatisticsUnavailableException;
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
 * @since 5.1.0
 */
public class DataNodeChooser {

    private static final Logger logger = LoggerFactory.getLogger(DataNodeChooser.class);

    public static ASTNode shard(ASTNode dne, Map<Integer, ParameterContext> parameterSettings,
                                Map<String, Comparable> extraCmd) throws QueryException {
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

    private static QueryTreeNode shardQuery(QueryTreeNode dne, Map<Integer, ParameterContext> parameterSettings,
                                            Map<String, Comparable> extraCmd) throws QueryException {
        if (dne instanceof QueryNode) {
            QueryNode q = (QueryNode) dne;
            if (q.getChild() != null) {
                QueryTreeNode child = q.getChild();
                child = shardQuery(child, parameterSettings, extraCmd);
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
            // shenxun : 这里的最后一个isWrite的属性，目前还未实现，所以，在切换时要全部停摆。
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
        } else if (dne instanceof JoinNode) {
            // Join节点应该在数据量大的一段进行
            boolean isPartitionOnPartition = false;
            JoinNode j = (JoinNode) dne;

            QueryTreeNode left = j.getLeftNode();
            QueryTreeNode right = j.getRightNode();

            String joinMergeJoinJudgeByRule = GeneralUtil.getExtraCmd(extraCmd,
                ExtraCmd.OptimizerExtraCmd.JoinMergeJoinJudgeByRule);

            if ("true".equalsIgnoreCase(joinMergeJoinJudgeByRule)) {
                if (right instanceof KVIndexNode) {
                    if (((KVIndexNode) right).getIndex().getPartitionColumns() == null
                        || ((KVIndexNode) right).getIndex().getPartitionColumns().isEmpty()) {
                        throw new OptimizerException("请在schema中配置" + ((KVIndexNode) right).getTableName() + "的分库键");
                    }

                    List<ISelectable> rightPartitionColumns = OptimizerUtils.columnMetaListToIColumnList(((KVIndexNode) right).getIndex()
                        .getPartitionColumns());

                    // 将partitionColumn中的别名表名做下替换
                    replaceTableNameAndAlias(rightPartitionColumns, right);
                    boolean isRuleEquals = false;
                    List<ISelectable> leftPartitionColumns = null;
                    if (left instanceof KVIndexNode) {
                        if (((KVIndexNode) left).getIndex().getPartitionColumns() == null
                            || ((KVIndexNode) left).getIndex().getPartitionColumns().isEmpty()) {
                            throw new OptimizerException("请在schema中配置" + ((KVIndexNode) left).getTableName() + "的分库键");
                        }

                        leftPartitionColumns = OptimizerUtils.columnMetaListToIColumnList(((KVIndexNode) left).getIndex()
                            .getPartitionColumns(),
                            ((KVIndexNode) left).getIndex().getTableName());
                        replaceTableNameAndAlias(leftPartitionColumns, left);
                        isRuleEquals = OptimizerContext.getContext()
                            .getRule()
                            .isSameRule(((KVIndexNode) right).getIndexName(), ((KVIndexNode) left).getIndexName());
                    } else if (left instanceof JoinNode) {
                        if (((JoinNode) left).getRightNode() instanceof KVIndexNode) {
                            leftPartitionColumns = OptimizerUtils.columnMetaListToIColumnList(((KVIndexNode) ((JoinNode) left).getRightNode()).getIndex()
                                .getPartitionColumns());

                            if (((KVIndexNode) ((JoinNode) left).getRightNode()).getIndex().getPartitionColumns() == null
                                || ((KVIndexNode) ((JoinNode) left).getRightNode()).getIndex()
                                    .getPartitionColumns()
                                    .isEmpty()) {
                                throw new OptimizerException("请在schema中配置"
                                                             + ((KVIndexNode) ((JoinNode) left).getRightNode()).getTableName()
                                                             + "的分库键");
                            }

                            replaceTableNameAndAlias(leftPartitionColumns,
                                ((KVIndexNode) ((JoinNode) left).getRightNode()));
                            replaceTableNameAndAlias(leftPartitionColumns, left);

                            for (IBooleanFilter joinFilter : j.getJoinFilter()) {
                                ISelectable leftColumn = (ISelectable) joinFilter.getColumn();
                                if (((JoinNode) left).isInnerJoin()) {
                                    if (((JoinNode) left).getRightNode().hasColumn(leftColumn)) {
                                        continue;
                                    }

                                    for (IBooleanFilter leftJoinFilter : ((JoinNode) left).getJoinFilter()) {
                                        if (leftJoinFilter.getColumn().equals(leftColumn)) {
                                            ISelectable c = ((ISelectable) leftJoinFilter.getValue()).copy();
                                            if (left.getAlias() != null) {
                                                c.setTableName(left.getAlias());
                                            }
                                            if (c.getAlias() != null) {
                                                c.setColumnName(c.getAlias()).setAlias(null);
                                            }
                                            joinFilter.setColumn(c);
                                        }
                                    }
                                }
                            }

                            isRuleEquals = OptimizerContext.getContext()
                                .getRule()
                                .isSameRule(((KVIndexNode) right).getIndexName(),
                                    ((KVIndexNode) ((JoinNode) left).getRightNode()).getIndexName());
                        } else {
                            throw new RuntimeException("这不可能");
                        }
                    } else {
                        throw new RuntimeException("这不可能");
                    }

                    if (isRuleEquals) {
                        int countOfPartitionColumnInJoinFilter = 0;
                        for (int i = 0; i < j.getJoinFilter().size(); i++) {
                            ISelectable leftColumnFromJoinFilter = (ISelectable) j.getJoinFilter().get(i).getColumn();
                            ISelectable rightColumnFromJoinFilter = (ISelectable) j.getJoinFilter().get(i).getValue();
                            int rightPartitionColumnIndex = rightPartitionColumns.indexOf(rightColumnFromJoinFilter);
                            if (rightPartitionColumnIndex < 0) {
                                continue;
                            }

                            int leftPartitionColumnIndex = leftPartitionColumns.indexOf(leftColumnFromJoinFilter);
                            if (leftPartitionColumnIndex < 0) {
                                continue;
                            }

                            if (rightPartitionColumnIndex == leftPartitionColumnIndex) {
                                countOfPartitionColumnInJoinFilter++;
                            }
                        }

                        if (countOfPartitionColumnInJoinFilter == rightPartitionColumns.size()) {
                            isPartitionOnPartition = true;
                        }
                    }
                } else {
                    isPartitionOnPartition = false;
                }
            }

            left = shardQuery(left, parameterSettings, extraCmd);
            right = shardQuery(right, parameterSettings, extraCmd);
            String joinMergeJoin = GeneralUtil.getExtraCmd(extraCmd, ExtraCmd.OptimizerExtraCmd.JoinMergeJoin);
            if ("true".equalsIgnoreCase(joinMergeJoin)) {
                isPartitionOnPartition = true;
            }

            /*
             * 如果是分库键join分库键，并且规则相同，则优化成join merge join
             */
            if (isPartitionOnPartition) {
                // 根据表名的后缀来判断两个表是不是对应的表
                // 由于是左树，所以右节点肯定是一个逻辑表的查询
                // 右节中如果是join（或者join的merge），则代表使用了索引
                // 这里先不要配索引，使右节点是KVIndexNode或者是KVIndexNode的merge
                // 左节点是join，则取左节点的右节点的后缀作为后缀
                Map<String, QueryTreeNode> lefts = new HashMap();
                if (left instanceof MergeNode) {
                    for (ASTNode child : left.getChildren()) {
                        String groupAndIdentifierOfTablePattern = null;
                        if (!(child instanceof KVIndexNode)) {
                            if (child instanceof JoinNode) {
                                if (((JoinNode) child).getRightNode() instanceof KVIndexNode) {
                                    groupAndIdentifierOfTablePattern = OptimizerUtils.getGroupAndIdentifierOfTablePattern((KVIndexNode) ((JoinNode) child).getRightNode());
                                } else {
                                    throw new RuntimeException("这里应该是KVIndexNode");
                                }
                            } else {
                                throw new RuntimeException("这里不应该是MergeNode");
                            }
                        } else {
                            groupAndIdentifierOfTablePattern = OptimizerUtils.getGroupAndIdentifierOfTablePattern((KVIndexNode) child);
                        }
                        child.setExtra(groupAndIdentifierOfTablePattern);
                        lefts.put((String) child.getExtra(), (QueryTreeNode) child);
                    }
                } else {
                    String groupAndIdentifierOfTablePattern = null;
                    if (!(left instanceof KVIndexNode)) {
                        if (left instanceof JoinNode) {
                            if (((JoinNode) left).getRightNode() instanceof KVIndexNode) {
                                groupAndIdentifierOfTablePattern = OptimizerUtils.getGroupAndIdentifierOfTablePattern((KVIndexNode) ((JoinNode) left).getRightNode());
                            } else {
                                throw new RuntimeException("这里应该是KVIndexNode");
                            }
                        } else {
                            throw new RuntimeException("这里不应该是MergeNode");
                        }
                    } else {
                        groupAndIdentifierOfTablePattern = OptimizerUtils.getGroupAndIdentifierOfTablePattern((KVIndexNode) left);
                    }
                    left.setExtra(groupAndIdentifierOfTablePattern);
                    lefts.put((String) left.getExtra(), left);
                }

                List<JoinNode> joins = new ArrayList();
                List<QueryTreeNode> rights = new ArrayList();
                if (right instanceof MergeNode) {
                    for (ASTNode child : right.getChildren()) {
                        if (!(child instanceof KVIndexNode)) {
                            throw new RuntimeException("右表不要配索引了");
                        }
                        rights.add((QueryTreeNode) child);
                        String groupAndIdentifierOfTablePattern = OptimizerUtils.getGroupAndIdentifierOfTablePattern((KVIndexNode) child);
                        child.setExtra(groupAndIdentifierOfTablePattern);
                    }
                } else {
                    if (!(right instanceof KVIndexNode)) {
                        throw new RuntimeException("右表不要配索引了");
                    }
                    String groupAndIdentifierOfTablePattern = OptimizerUtils.getGroupAndIdentifierOfTablePattern((KVIndexNode) right);
                    right.setExtra(groupAndIdentifierOfTablePattern);
                    rights.add(right);
                }

                // 根据后缀，找到匹配的表，生成join
                for (QueryTreeNode r : rights) {
                    QueryTreeNode l = lefts.get(r.getExtra());
                    if (l == null) {
                        continue;
                    }
                    JoinNode newj = j.copy();
                    newj.setLeftNode(l);
                    newj.setRightNode(r);
                    newj.executeOn(l.getDataNode());
                    joins.add(newj);
                }

                if (joins.size() > 1) {
                    MergeNode merge = new MergeNode();
                    for (JoinNode join : joins) {
                        merge.merge(join);
                    }

                    merge.executeOn(joins.get(0).getDataNode()).setExtra(joins.get(0).getExtra());
                    merge.build();
                    return merge;
                } else if (joins.size() == 1) {
                    return joins.get(0);
                } else if (joins.isEmpty()) {
                    // 左右两表在不同的库上
                    throw new EmptyResultFilterException("空结果");

                }
            }

            j.setLeftNode(left);
            // NestedLoop情况下
            // 如果右边是多个表，则分库需要再执行层根据左边的结果做
            if (!(right instanceof MergeNode && (j.getJoinStrategy().getType().equals(JoinType.INDEX_NEST_LOOP) || j.getJoinStrategy()
                .getType()
                .equals(JoinType.NEST_LOOP_JOIN)))) {
                j.setRightNode(right);
            } else {
                if (right.isSubQuery()) {
                    j.setRightNode(right);
                } else {
                    right = (QueryTreeNode) new MergeNode();
                    ((MergeNode) right).merge(j.getRightNode());
                    ((MergeNode) right).setSharded(false);
                    j.getRightNode().executeOn("undecided");

                    right.build();
                    j.setRightNode(right);
                }
            }

            String dataNode = j.getLeftNode().getDataNode();
            // 对于未决的IndexNestedLoop，join应该在左节点执行
            if (right instanceof MergeNode && !((MergeNode) right).isSharded()) {
                dataNode = j.getLeftNode().getDataNode();
                j.executeOn(dataNode);
                right.executeOn(j.getDataNode());
            } else {
                try {
                    Cost leftCost;
                    leftCost = CostEsitimaterFactory.estimate(left);
                    Cost rightCost = CostEsitimaterFactory.estimate(right);
                    dataNode = leftCost.getRowCount() > rightCost.getRowCount() ? j.getLeftNode().getDataNode() : j.getRightNode()
                        .getDataNode();
                    j.executeOn(dataNode);
                } catch (StatisticsUnavailableException e) {
                    throw new OptimizerException(e);
                }
            }

            return j;
        } else if (dne instanceof MergeNode) {
            // 为merge选择执行节点
            // 很多方案...
            // 两路归并?
            // 都发到一台机器上？
            // 目前的方案是都发到一台机器上
            // 对Merge的每个子节点的rowCount进行排序
            // 找出rowCount最大的子节点
            // merge应该在该机器上执行，其他机器的数据都发送给它
            MergeNode m = (MergeNode) dne;
            List<ASTNode> subNodes = m.getChildren();
            m = new MergeNode();
            m.setUnion(((MergeNode) dne).isUnion());
            long maxRowCount = 0;
            String maxRowCountDataNode = subNodes.get(0).getDataNode();
            for (int i = 0; i < subNodes.size(); i++) {
                QueryTreeNode child = (QueryTreeNode) subNodes.get(i);
                child = shardQuery(child, parameterSettings, extraCmd);
                subNodes.set(i, child);
                try {
                    Cost childCost;
                    childCost = CostEsitimaterFactory.estimate(child);
                    if (childCost.getRowCount() > maxRowCount) {
                        maxRowCount = childCost.getRowCount();
                        maxRowCountDataNode = child.getDataNode();
                    }
                } catch (StatisticsUnavailableException e) {
                    throw new OptimizerException(e);
                }

            }

            if (maxRowCountDataNode == null) {
                maxRowCountDataNode = subNodes.get(0).getDataNode();
            }

            m.executeOn(maxRowCountDataNode);
            m.merge(subNodes);
            m.setSharded(true);
            m.build();
            return m;
        }

        return dne;
    }

    private static void replaceTableNameAndAlias(List<ISelectable> columns, QueryTreeNode node) {
        List<ISelectable> rightSelected = node.getColumnsSelected();
        for (ISelectable rightPartitionColumn : columns) {
            if (node.getAlias() != null) {
                rightPartitionColumn.setTableName(node.getAlias());
            }

            int index = rightSelected.indexOf(rightPartitionColumn);
            if (index > -1) {
                ISelectable rightPartitionColumnFromRightSelected = rightSelected.get(index);
                if (rightPartitionColumnFromRightSelected.getAlias() != null) {
                    rightPartitionColumn.setColumnName(rightPartitionColumnFromRightSelected.getAlias());
                }
            }
        }

    }

    private static ASTNode shardInsert(InsertNode dne, Map<Integer, ParameterContext> parameterSettings,
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

    private static ASTNode shardUpdate(UpdateNode dne, Map<Integer, ParameterContext> parameterSettings,
                                       Map<String, Comparable> extraCmd) throws QueryException {
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
                                       Map<String, Comparable> extraCmd) throws QueryException {
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

    // =============== helper method =================

    private static List<TargetDB> shard(String logicalTableName, String logicalIndexName, IFilter f,
                                        boolean isTraceSource, boolean isWrite) {
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

    private static QueryTreeNode buildMerge(TableNode q, List<TargetDB> dataNodeChoosed, boolean isIn, Object column)
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

    private static OneDbNodeWithCount buildMergeInOneDB(String executeOn, TableNode q, Map<String, Field> tabMap,
                                                        boolean isIn, Object column, boolean needCopy)
                                                                                                      throws QueryException {

        long totalRowCount = 0;
        OneDbNodeWithCount oneDbNodeWithCount = new OneDbNodeWithCount();

        for (String targetTable : tabMap.keySet()) {
            TableNode sub = null;
            if (needCopy) {
                sub = q.copy();
            } else {
                sub = q;
            }

            // tddl的traceSource在分库不分表，和全表扫描时无法使用
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
            sub.setActualTableName(targetTable);
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

    private static void traceSourceInFilter(IFilter filter, Map<String, Set<Object>> sourceKeys) {
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

    private static UpdateNode buildOneQueryUpdate(QueryTreeNode sub, List columns, List values) {
        if (sub instanceof TableNode) {
            UpdateNode update = ((TableNode) sub).update(columns, values);
            update.executeOn(sub.getDataNode());
            return update;
        } else {
            throw new UnsupportedOperationException("update中暂不支持按照索引进行查询");
        }
    }

    private static DeleteNode buildOneQueryDelete(QueryTreeNode sub) {
        if (sub instanceof TableNode) {
            DeleteNode delete = ((TableNode) sub).delete();
            delete.executeOn(sub.getDataNode());
            return delete;
        } else {
            throw new UnsupportedOperationException("delete中暂不支持按照索引进行查询");
        }
    }

}
