package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.IMergeCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.DistinctCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IParallelizableQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

public class MergeHandler extends QueryHandlerCommon {

    public MergeHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {
        // todo: 处理聚合函数生成merge.
        // merge，多个相同schema cursor的合并操作。
        IMerge merge = (IMerge) executor;
        IRepository repo = executionContext.getCurrentRepository();
        List<IDataNodeExecutor> subNodes = merge.getSubNode();
        List<ISchematicCursor> subCursors = new ArrayList<ISchematicCursor>();
        if (!merge.isSharded()) {
            /*
             * 如果是个需要左驱动表的结果来进行查询的查询，直接返回mergeCursor.
             * 有些查询，是需要依赖左值结果进行查询的。这类查询需要先取一批左值出来，根据这些左值，走规则算右值的。
             * 这时候只有一个subNodes
             */
            if (subNodes.size() != 1) {
                throw new IllegalArgumentException("subNodes is not 1? may be 执行计划生育上有了问题了，查一下" + executor);
            }
            ExecutionContext tempContext = new ExecutionContext();
            tempContext.setCurrentRepository(executionContext.getCurrentRepository());
            IQuery ide = (IQuery) subNodes.get(0);
            buildTableAndMetaLogicalIndex(ide, tempContext);
            // IndexMeta indexMeta = tempContext.getMeta();
            // ICursorMeta iCursorMetaTemp =
            // GeneralUtil.convertToICursorMeta(indexMeta);
            ICursorMeta iCursorMetaTemp = ExecUtils.convertToICursorMeta(ide);

            // ColumnMeta[] keyColumns = indexMeta.getKeyColumns();
            ColumnMeta[] keyColumns = new ColumnMeta[] {};

            List<IOrderBy> tempOrderBy = new LinkedList<IOrderBy>();
            for (ColumnMeta cm : keyColumns) {
                IOrderBy ob = ASTNodeFactory.getInstance().createOrderBy();
                ob.setColumn(ExecUtils.getIColumnsFromColumnMeta(cm, ide.getAlias()));
                tempOrderBy.add(ob);
            }
            cursor = repo.getCursorFactory().mergeCursor(executionContext,
                subCursors,
                iCursorMetaTemp,
                subNodes.get(0),
                tempOrderBy);
            return cursor;
        } else {
            if (QUERY_CONCURRENCY.CONCURRENT == merge.getQueryConcurrency()) {
                executeSubNodesFuture(cursor, executionContext, subNodes, subCursors);
            } else {
                executeSubNodes(cursor, executionContext, subNodes, subCursors);

            }
        }
        if (subNodes.get(0) instanceof IPut) {// 合并affect_rows
            int affect_rows = 0;
            for (ISchematicCursor affectRowCursor : subCursors) {
                IRowSet rowSet = affectRowCursor.next();
                Integer affectRow = rowSet.getInteger(0);
                if (affectRow != null) {
                    affect_rows += affectRow;
                }
                List<TddlException> exs = new ArrayList();
                exs = affectRowCursor.close(exs);
                if (!exs.isEmpty()) throw (GeneralUtil.mergeException(exs));
            }
            IAffectRowCursor affectRow = repo.getCursorFactory().affectRowCursor(executionContext, affect_rows);
            return affectRow;
        } else {

            // union的话要去重
            // 这里假设都是排好序的
            if (merge.isUnion()) {
                cursor = this.buildMergeSortCursor(executionContext, repo, subCursors, false);
            } else {
                cursor = repo.getCursorFactory().mergeCursor(executionContext, subCursors, executor);
            }
        }
        return cursor;
    }

    private void executeSubNodes(ISchematicCursor cursor, ExecutionContext executionContext,
                                 List<IDataNodeExecutor> subNodes, List<ISchematicCursor> subCursors)
                                                                                                     throws TddlException {
        for (IDataNodeExecutor q : subNodes) {
            ISchematicCursor rc = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNode(q, executionContext);
            subCursors.add(rc);
        }
    }

    @SuppressWarnings("rawtypes")
    private void executeSubNodesFuture(ISchematicCursor cursor, ExecutionContext executionContext,
                                       List<IDataNodeExecutor> subNodes, List<ISchematicCursor> subCursors)
                                                                                                           throws TddlException {

        executionContext.getExtraCmds().put(ExtraCmd.EXECUTE_QUERY_WHEN_CREATED, "True");
        List<Future<ISchematicCursor>> futureCursors = new LinkedList<Future<ISchematicCursor>>();
        for (IDataNodeExecutor q : subNodes) {
            Future<ISchematicCursor> rcfuture = executeFuture(executionContext, q);
            futureCursors.add(rcfuture);
        }
        for (Future<ISchematicCursor> future : futureCursors) {
            try {
                subCursors.add(future.get(15, TimeUnit.MINUTES));
            } catch (Exception e) {
                throw new TddlException(e);
            }
        }
    }

    /**
     * 先进行合并，然后进行aggregats
     * 
     * @param cursor
     * @param context
     * @param executor
     * @param closeResultCursor
     * @param repo
     * @param executionContext
     * @return
     * @throws TddlException
     */
    private ISchematicCursor executeMergeAgg(ISchematicCursor cursor, IDataNodeExecutor executor,
                                             boolean closeResultCursor, IRepository repo, List<IOrderBy> groupBycols,
                                             ExecutionContext executionContext) throws TddlException {
        List _retColumns = ((IQueryTree) executor).getColumns();
        if (_retColumns != null) {
            List<IFunction> aggregates = getMergeAggregates(_retColumns);
            for (IFunction aggregate : aggregates) {

                if (aggregate.isNeedDistinctArg()) {
                    IQueryTree sub = (IQueryTree) ((IMerge) executor).getSubNode().get(0);

                    // 这时候的order by是sub对外的order by，要做好别名替换
                    List<ISelectable> columns = ExecUtils.copySelectables(sub.getColumns());
                    for (ISelectable c : columns) {
                        c.setTableName(sub.getAlias());
                        if (c.getAlias() != null) {
                            c.setColumnName(c.getAlias());
                            c.setAlias(null);
                        }
                    }

                    cursor = this.processOrderBy(cursor,
                        getOrderBy(columns),
                        executionContext,
                        (IQueryTree) executor,
                        true);
                    cursor = new DistinctCursor(cursor, getOrderBy(columns));
                    break;
                }
            }

            if ((aggregates != null && !aggregates.isEmpty()) || (groupBycols != null && !groupBycols.isEmpty())) {
                cursor = repo.getCursorFactory().aggregateCursor(executionContext,
                    cursor,
                    aggregates,
                    groupBycols,
                    _retColumns,
                    true);
            }
        }
        return cursor;
    }

    @Override
    protected ISchematicCursor executeAgg(ISchematicCursor cursor, IDataNodeExecutor executor,
                                          boolean closeResultCursor, IRepository repo, List<IFunction> aggregates,
                                          List<IOrderBy> groupBycols, ExecutionContext executionContext)
                                                                                                        throws TddlException {
        return this.executeMergeAgg(cursor, executor, closeResultCursor, repo, groupBycols, executionContext);
    }

    @Override
    protected ISchematicCursor processOrderBy(ISchematicCursor cursor, List<IOrderBy> ordersInRequest,
                                              ExecutionContext executionContext, IQueryTree query,
                                              boolean needOrderMatch) throws TddlException {
        IRepository repo = executionContext.getCurrentRepository();
        // TODO shenxun: 临时表问题修复
        boolean hasOrderBy = ordersInRequest != null && !ordersInRequest.isEmpty();
        if (!hasOrderBy) {
            return cursor;
        }
        if (cursor instanceof IMergeCursor) {
            IMergeCursor mergeCursor = (IMergeCursor) cursor;
            List<ISchematicCursor> cursors = mergeCursor.getISchematicCursors();
            /*
             * 所有子节点，如果都是顺序，则可以直接使用mergeSort进行合并排序。 如果都是
             * 逆序，则不符合预期，用临时表（因为优化器应该做优化，尽可能将子cursor的顺序先变成正续，这里不会出现这种情况）
             * 如果有正有逆序，使用临时表 其他情况，使用临时表
             */
            OrderByResult tempOBR = null;
            for (ISchematicCursor subCur : cursors) {
                OrderByResult obR = chooseOrderByMethod(subCur, ordersInRequest, executionContext, needOrderMatch);
                if (tempOBR == null) {
                    tempOBR = obR;
                }
                if (obR != OrderByResult.normal) {
                    tempOBR = OrderByResult.temporaryTable;
                }
            }
            if (tempOBR == OrderByResult.normal) {// 正常的合并.
                // 不去重的
                cursor = buildMergeSortCursor(executionContext, repo, cursors, true);
            } else if (tempOBR == OrderByResult.temporaryTable || tempOBR == OrderByResult.reverseCursor) {

                cursor = repo.getCursorFactory().tempTableSortCursor(executionContext,
                    cursor,
                    ordersInRequest,
                    true,
                    query.getRequestID());

            } else {
                throw new IllegalArgumentException("shoult not be here:" + tempOBR);
            }
        } else {
            return super.processOrderBy(cursor, ordersInRequest, executionContext, query, needOrderMatch);

        }
        return cursor;
        /*
         * cursor = repo.getCursorFactory().heapSortCursor( (ISchematicCursor)
         * cursor, _orderBy, _from, _from + _limit);
         */
    }

    private ISchematicCursor buildMergeSortCursor(ExecutionContext executionContext, IRepository repo,
                                                  List<ISchematicCursor> cursors, boolean duplicated)
                                                                                                     throws TddlException {
        ISchematicCursor cursor;
        cursor = repo.getCursorFactory().mergeSortedCursor(executionContext, cursors, duplicated);
        return cursor;
    }
}
