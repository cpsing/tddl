package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Repository;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

public class NestedLoopJoinHandler extends QueryHandlerCommon {

    public NestedLoopJoinHandler(){
        super();
    }

    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {

        IJoin join = (IJoin) executor;
        Repository repo = executionContext.getCurrentRepository();
        IQueryTree leftQuery = join.getLeftNode();

        ISchematicCursor cursor_left = ExecutorContext.getContext()
            .getTransactionExecutor()
            .execByExecPlanNode(leftQuery, executionContext);

        ISchematicCursor cursor_right = ExecutorContext.getContext()
            .getTransactionExecutor()
            .execByExecPlanNode(join.getRightNode(), executionContext);

        cursor = repo.getCursorFactory().join_blockNestedLoopCursor(executionContext,
            cursor_left,
            cursor_right,
            join.getLeftJoinOnColumns(),
            join.getRightJoinOnColumns(),
            join.getColumns(),
            join);
        return cursor;
    }

}
