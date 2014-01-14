package com.taobao.tddl.executor.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.RangeCursor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 用于处理执行的handler 。 目的是将执行计划中的Query节点进行转义处理。
 * 这个执行器主要是用在使用KV接口的数据库里，比如bdb.concurrentHashMap等。
 * 
 * @author Whisper
 */
public class QueryHandler extends QueryHandlerCommon {

    public QueryHandler(){
        super();
    }

    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {
        List<IOrderBy> _orderBy = ((IQueryTree) executor).getOrderBys();
        IRepository repo = executionContext.getCurrentRepository();
        IDataNodeExecutor _subQuery = null;
        IQuery query = (IQuery) executor;
        _subQuery = query.getSubQuery();
        if (_subQuery != null) {
            // 如果有subQuery,则按照subquery构建
            cursor = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(_subQuery, executionContext);
        } else {
            ITable table = null;
            IndexMeta meta = null;
            buildTableAndMeta(query, executionContext);
            table = executionContext.getTable();
            meta = executionContext.getMeta();
            cursor = table.getCursor(executionContext, meta, (IQuery) executor);
        }

        // 获得查询结果的元数据
        // 获得本次查询的keyFilter
        IFilter keyFilter = query.getKeyFilter();
        if (keyFilter != null) {
            if (keyFilter instanceof IBooleanFilter) {
                // 外键约束好像没用
                // if (meta.getRelationship() == Relationship.MANY_TO_MANY) {
                // cursor = manageToReverseIndex(cursor, executor, repo,
                // transaction, executionContext.getTable(), meta,
                // keyFilter);
                // } else {}
                // 非倒排索引,即普通索引,走的查询方式
                cursor = manageToBooleanRangeCursor(executionContext, cursor, repo, keyFilter);
            } else if (keyFilter instanceof ILogicalFilter) {
                ILogicalFilter lf = (ILogicalFilter) keyFilter;
                cursor = repo.getCursorFactory().rangeCursor(executionContext, cursor, lf);
            }

            if (cursor instanceof RangeCursor) {//
                if (_orderBy != null) {
                    if (_orderBy.size() == 1) {
                        IOrderBy o1 = _orderBy.get(0);
                        IOrderBy o2 = cursor.getOrderBy().get(0);
                        boolean needSort = !equalsIOrderBy(o1, o2);
                        boolean direction = o1.getDirection();
                        if (!needSort) {
                            if (!direction) {
                                // DescRangeCursor
                                cursor = repo.getCursorFactory().reverseOrderCursor(executionContext, cursor);
                            } else {
                                // asc,default
                            }
                            _orderBy = null;
                        }
                    }
                }
            }
        }
        return cursor;
    }

    protected ISchematicCursor manageToBooleanRangeCursor(ExecutionContext executionContext, ISchematicCursor cursor,
                                                          IRepository repo, IFilter keyFilter) throws TddlException {
        IBooleanFilter bf = (IBooleanFilter) keyFilter;
        IColumn c = ExecUtils.getColumn(bf.getColumn());
        OPERATION op = bf.getOperation();
        if (op == OPERATION.IN) {
            List<Object> values = bf.getValues();
            if (values == null) {
                throw new IllegalArgumentException("values is null ,but operation is in . logical error");
            } else {
                return repo.getCursorFactory().inCursor(executionContext, cursor, cursor.getOrderBy(), c, values, op);
            }
        }
        try {
            cursor = repo.getCursorFactory().rangeCursor(executionContext, cursor, keyFilter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }

    protected ISchematicCursor manageToReverseIndex(ExecutionContext executionContext, ISchematicCursor cursor,
                                                    IDataNodeExecutor executor, IRepository repo,
                                                    ITransaction transaction, ITable table, IndexMeta meta,
                                                    IFilter keyFilter) throws TddlException {
        throw new IllegalArgumentException("should not be here");

    }
}
