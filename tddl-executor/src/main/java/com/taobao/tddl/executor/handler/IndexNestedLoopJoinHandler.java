package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Repository;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

public class IndexNestedLoopJoinHandler extends QueryHandlerCommon {

    public IndexNestedLoopJoinHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {
        return doIndexNestLoop(cursor, executor, executionContext);
    }

    @SuppressWarnings("rawtypes")
    protected ISchematicCursor doIndexNestLoop(ISchematicCursor cursor, IDataNodeExecutor executor,
                                               ExecutionContext executionContext) throws TddlException {
        // 默认右节点是有索引的。
        IJoin join = (IJoin) executor;
        Repository repo = executionContext.getCurrentRepository();
        IQueryTree leftQuery = join.getLeftNode();

        ISchematicCursor cursor_left = null;
        ISchematicCursor cursor_right = null;

        try {

            cursor_left = ExecutorContext.getContext()
                .getTransactionExecutor()
                .execByExecPlanNode(leftQuery, executionContext);

            cursor_right = ExecutorContext.getContext()
                .getTransactionExecutor()
                .execByExecPlanNode(join.getRightNode(), executionContext);

        } catch (RuntimeException e) {
            List<TddlException> exs = new ArrayList();
            if (cursor_left != null) {
                exs = cursor_left.close(exs);
            }
            if (cursor_right != null) {
                exs = cursor_right.close(exs);
            }

            if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
            throw e;
        }

        int n = 0;
        cursor = repo.getCursorFactory().indexNestLoopCursor(executionContext,
            cursor_left,
            cursor_right,
            join.getLeftJoinOnColumns(),
            join.getRightJoinOnColumns(),
            join.getColumns(),
            n == 1,
            (IJoin) executor);
        return cursor;
    }
}
