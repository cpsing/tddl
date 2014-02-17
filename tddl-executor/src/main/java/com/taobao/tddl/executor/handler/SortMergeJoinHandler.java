package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

public class SortMergeJoinHandler extends QueryHandlerCommon {

    public SortMergeJoinHandler(){
        super();
    }

    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {
        IJoin join = (IJoin) executor;
        // 左面的查询
        final IQueryTree leftQuery = join.getLeftNode();
        // 右面的查询
        final IQueryTree rightQuery = join.getRightNode();
        ISchematicCursor cursor_left = null;
        ISchematicCursor cursor_right = null;
        int left_match = -1;
        int right_match = -1;
        IRepository repo = executionContext.getCurrentRepository();
        // 查询左面节点。
        cursor_left = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(leftQuery, executionContext);
        /*
         * 如果左边的join on 【columns】 里面的columns,和数据的自然排序相同，则可以直接使用。 否则，使用临时表。
         */
        left_match = matchIndex(getOrderBy(join.getLeftJoinOnColumns()), cursor_left.getOrderBy());
        if (left_match == NOT_MATCH) {
            cursor_left = repo.getCursorFactory().tempTableSortCursor(executionContext,
                cursor_left,
                getOrderBy(join.getLeftJoinOnColumns()),
                true,
                executor.getRequestID());
            left_match = MATCH;
        }
        /*
         * 同上
         */
        cursor_right = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(rightQuery, executionContext);
        right_match = matchIndex(getOrderBy(join.getRightJoinOnColumns()), cursor_right.getOrderBy());
        if (right_match == NOT_MATCH) {
            // 这里是，排序不匹配，所以使用leftJoinOnColumns进行临时表构建。
            cursor_right = repo.getCursorFactory().tempTableSortCursor(executionContext,
                cursor_right,
                getOrderBy(join.getRightJoinOnColumns()),
                true,
                executor.getRequestID());

            right_match = MATCH;
        }
        // 构造sortMerge Join的cursor
        cursor = repo.getCursorFactory().sortMergeJoinCursor(executionContext,
            cursor_left,
            cursor_right,
            join.getLeftJoinOnColumns(),
            join.getRightJoinOnColumns(),
            join);
        return cursor;
    }

}
